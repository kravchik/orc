from __future__ import annotations

import threading
from typing import Any, Callable

from orchestrator import clock
from orchestrator.access_point_common import (
    ACCESS_POINT_OUTBOUND_CLASS_CALLBACK_ACK,
    ACCESS_POINT_OUTBOUND_CLASS_EDIT,
    ACCESS_POINT_OUTBOUND_CLASS_SEND,
    AccessPointDeliveryReceipt,
    AccessPointDeliveryState,
    QueuedAccessPointOutbound,
    access_point_outbound_sort_key,
)


class TelegramDeliveryLane:
    """Serializes Telegram outbound transport work behind receipts."""

    def __init__(self, *, logger, source: str) -> None:
        self._logger = logger
        self._source = source
        self._lock = threading.RLock()
        self._cv = threading.Condition(self._lock)
        self._outbound_queue: list[QueuedAccessPointOutbound[object | None]] = []
        self._queued_keys: set[tuple[Any, ...]] = set()
        self._receipts: dict[int, AccessPointDeliveryReceipt] = {}
        self._next_receipt_id = 1
        self._next_sequence = 1
        self._stop = False
        self._worker_started = False
        self._worker = threading.Thread(
            target=self._run,
            name=f"telegram-delivery-lane:{source}",
            daemon=True,
        )

    def enqueue(
        self,
        *,
        operation: str | None,
        execute: Callable[[], object | None],
        priority: int = ACCESS_POINT_OUTBOUND_CLASS_SEND,
        coalesce_key: tuple[Any, ...] | None = None,
        progress_on_success: bool = True,
        on_success: Callable[[object | None], None] | None = None,
        on_failure: Callable[[Exception], None] | None = None,
    ) -> int:
        with self._cv:
            self._ensure_worker_started()
            if coalesce_key is not None and coalesce_key in self._queued_keys:
                for item in self._outbound_queue:
                    if item.coalesce_key == coalesce_key:
                        return item.receipt_id
            receipt_id = self._next_receipt_id
            self._next_receipt_id += 1
            sequence = self._next_sequence
            self._next_sequence += 1
            self._receipts[receipt_id] = AccessPointDeliveryReceipt(receipt_id=receipt_id)
            item = QueuedAccessPointOutbound(
                receipt_id=receipt_id,
                priority=int(priority),
                sequence=int(sequence),
                operation=operation,
                coalesce_key=coalesce_key,
                progress_on_success=bool(progress_on_success),
                execute=execute,
                on_success=on_success,
                on_failure=on_failure,
            )
            self._outbound_queue.append(item)
            if coalesce_key is not None:
                self._queued_keys.add(coalesce_key)
            self._cv.notify_all()
            return receipt_id

    def enqueue_send_text(
        self,
        *,
        output_runtime,
        chat_id: int,
        text: str,
        kind: str,
        reply_markup: dict | None = None,
        parse_mode: str | None = None,
        raise_on_error: bool = False,
        priority: int = 0,
        on_failure: Callable[[Exception], None] | None = None,
    ) -> int:
        return self.enqueue(
            operation="send_text",
            execute=lambda out=output_runtime, cid=chat_id, body=text, k=kind, markup=reply_markup, mode=parse_mode, roe=raise_on_error: out.send_text(
                chat_id=cid,
                text=body,
                kind=k,
                reply_markup=markup,
                parse_mode=mode,
                raise_on_error=roe,
            ),
            priority=priority,
            on_failure=on_failure,
        )

    def enqueue_answer_callback_query(
        self,
        *,
        client,
        callback_query_id: str,
        text: str | None = None,
        priority: int = ACCESS_POINT_OUTBOUND_CLASS_CALLBACK_ACK,
        on_failure: Callable[[Exception], None] | None = None,
    ) -> int:
        return self.enqueue(
            operation="answer_callback_query",
            execute=lambda base=client, cbid=callback_query_id, body=text: base.answer_callback_query(cbid, text=body),
            priority=priority,
            on_failure=on_failure,
        )

    def enqueue_edit_message_reply_markup(
        self,
        *,
        client,
        chat_id: int,
        message_id: int,
        reply_markup: dict | None,
        priority: int = ACCESS_POINT_OUTBOUND_CLASS_EDIT,
        on_failure: Callable[[Exception], None] | None = None,
    ) -> int:
        return self.enqueue(
            operation="edit_message_reply_markup",
            execute=lambda base=client, cid=chat_id, mid=message_id, markup=reply_markup: base.edit_message_reply_markup(
                chat_id=cid,
                message_id=mid,
                reply_markup=markup,
            ),
            priority=priority,
            on_failure=on_failure,
        )

    def wait(self, receipt_id: int, *, timeout_sec: float | None = None) -> AccessPointDeliveryReceipt:
        with self._cv:
            receipt = self._receipts[receipt_id]
            if receipt.state != AccessPointDeliveryState.PENDING:
                return receipt
            deadline = None if timeout_sec is None else (float(clock.monotonic()) + float(timeout_sec))
            while receipt.state == AccessPointDeliveryState.PENDING:
                remaining = None if deadline is None else max(0.0, deadline - float(clock.monotonic()))
                if remaining is not None and remaining <= 0.0:
                    raise TimeoutError(f"telegram delivery receipt {receipt_id} timed out")
                self._cv.wait(timeout=remaining)
            return receipt

    def stop(self, *, drain: bool = False, timeout_sec: float = 1.0) -> None:
        thread: threading.Thread | None = None
        with self._cv:
            if not self._worker_started:
                return
            if drain:
                deadline = float(clock.monotonic()) + max(0.0, float(timeout_sec))
                while self._outbound_queue and float(clock.monotonic()) < deadline:
                    self._cv.wait(timeout=0.01)
            if self._outbound_queue:
                for item in self._outbound_queue:
                    receipt = self._receipts.get(item.receipt_id)
                    if receipt is not None and receipt.state == AccessPointDeliveryState.PENDING:
                        receipt.mark_cancelled()
                    if item.coalesce_key is not None:
                        self._queued_keys.discard(item.coalesce_key)
                self._outbound_queue.clear()
            if not self._worker_started:
                return
            self._stop = True
            thread = self._worker
            self._cv.notify_all()
        if thread is not None:
            thread.join(timeout=max(0.1, float(timeout_sec)))

    def _ensure_worker_started(self) -> None:
        if self._worker_started:
            return
        self._worker_started = True
        self._worker.start()

    def _take_next_item(self) -> QueuedAccessPointOutbound[object | None] | None:
        if not self._outbound_queue:
            return None
        self._outbound_queue.sort(key=access_point_outbound_sort_key)
        return self._outbound_queue.pop(0)

    def _run(self) -> None:
        while True:
            with self._cv:
                while not self._outbound_queue and not self._stop:
                    self._cv.wait()
                if self._stop and not self._outbound_queue:
                    return
                item = self._take_next_item()
                if item is None:
                    continue
            try:
                result = item.execute()
            except Exception as exc:
                self._mark_failed(item, exc)
                continue
            self._mark_sent(item, result)

    def _mark_sent(self, item: QueuedAccessPointOutbound[object | None], result: object | None) -> None:
        with self._cv:
            receipt = self._receipts[item.receipt_id]
            message_id = None
            message_ts = None
            if isinstance(result, dict):
                raw_message_id = result.get("message_id")
                if isinstance(raw_message_id, int):
                    message_id = raw_message_id
                raw_ts = result.get("ts")
                if isinstance(raw_ts, str) and raw_ts.strip():
                    message_ts = raw_ts.strip()
            else:
                raw_message_id = getattr(result, "first_message_id", None)
                if isinstance(raw_message_id, int):
                    message_id = raw_message_id
                raw_ts = getattr(result, "message_ts", None)
                if isinstance(raw_ts, str) and raw_ts.strip():
                    message_ts = raw_ts.strip()
            receipt.mark_sent(message_id=message_id, message_ts=message_ts)
            if item.coalesce_key is not None:
                self._queued_keys.discard(item.coalesce_key)
            self._cv.notify_all()
        if callable(item.on_success):
            item.on_success(result)

    def _mark_failed(self, item: QueuedAccessPointOutbound[object | None], exc: Exception) -> None:
        with self._cv:
            receipt = self._receipts[item.receipt_id]
            receipt.mark_failed(exc)
            if item.coalesce_key is not None:
                self._queued_keys.discard(item.coalesce_key)
            self._cv.notify_all()
        if callable(item.on_failure):
            item.on_failure(exc)
        self._logger.event(
            "telegram_delivery_lane_error",
            source=self._source,
            operation=item.operation,
            error=str(exc),
            error_type=type(exc).__name__,
        )
