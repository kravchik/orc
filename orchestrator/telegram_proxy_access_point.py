"""Telegram access-point adapter for shared proxy core."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, TYPE_CHECKING

from orchestrator.access_point_common import (
    ACCESS_POINT_OUTBOUND_CLASS_APPROVAL_CLEANUP,
    ACCESS_POINT_OUTBOUND_CLASS_CALLBACK_ACK,
    ACCESS_POINT_OUTBOUND_CLASS_EDIT,
    ACCESS_POINT_OUTBOUND_CLASS_SEND,
    AccessPointDeliveryReceipt,
    AccessPointDeliveryState,
    QueuedAccessPointOutbound,
    access_point_outbound_sort_key,
)
from orchestrator.approval import build_accept_settings
from orchestrator.proxy_access_point import ProxyApprovalDecision, ProxyModelSelection, ProxySubmitPrompt
from orchestrator.telegram_approval_helper import _format_approval_details, _format_approval_prompt
from orchestrator.telegram_output_runtime import TelegramKind, TelegramOutputRuntime
from orchestrator.turn_status_store import TurnStatusStore

if TYPE_CHECKING:
    from orchestrator.telegram_agent import (
        TelegramAgentCore,
        _TelegramAgentResult,
        _TelegramApprovalPromptEvent,
        _TelegramModelApplyResult,
        _TelegramOutboundNote,
        _TelegramStatusEvent,
    )
    from orchestrator.telegram_bridge import TelegramCallbackUpdate, TelegramTextUpdate


@dataclass(frozen=True)
class _TelegramProxyOutboundResult:
    sent: bool
    first_message_id: int | None = None
    sent_chunks: int = 0


class TelegramAccessPointAdapter:
    """Telegram-specific inbound and outbound AP behavior above shared proxy core."""

    def __init__(self, *, core: "TelegramAgentCore") -> None:
        self._core = core
        self._outbound_queue: list[QueuedAccessPointOutbound[_TelegramProxyOutboundResult]] = []
        self._next_outbound_receipt_id = 1
        self._next_outbound_sequence = 1
        self._queued_keys: set[tuple[Any, ...]] = set()
        self._receipts: dict[int, AccessPointDeliveryReceipt] = {}

    def send_result(self, result: "_TelegramAgentResult") -> None:
        self._enqueue_send_text(
            chat_id=result.chat_id,
            thread_id=result.thread_id,
            text=result.reply,
            kind=TelegramKind.REPLY,
        )

    def after_record_result(self, result: "_TelegramAgentResult") -> None:
        if _normalize_command(result.prompt) == "/compact":
            self._core._logger.event(
                "compact_result",
                source="telegram_agent",
                chat_id=result.chat_id,
                thread_id=result.thread_id,
                result="completed",
            )

    def format_result_log(self, result: "_TelegramAgentResult") -> str:
        return f"chat_id={result.chat_id} thread_id={result.thread_id} in={result.prompt!r} out={result.reply!r}"

    def record_model_apply_result(self, result: "_TelegramModelApplyResult") -> None:
        self._core.set_awaiting(True)
        self._core._logger.event(
            "model_apply_result",
            source="telegram_agent",
            chat_id=result.chat_id,
            thread_id=result.thread_id,
            requested_model=result.requested_model,
            actual_model=result.actual_model,
            result=result.result,
            error=result.error,
        )
        text = (
            f"model applied: {result.actual_model or result.requested_model}"
            if result.result == "completed"
            else f"model apply failed: {result.error or 'unsupported'}"
        )
        self._enqueue_send_text(
            chat_id=result.chat_id,
            thread_id=result.thread_id,
            text=text,
            kind=TelegramKind.COMMAND,
        )

    def send_approval_prompt(self, event: "_TelegramApprovalPromptEvent") -> dict[str, Any]:
        allow_always = build_accept_settings(params=event.request.params) is not None
        keyboard_buttons = [
            {"text": "Accept", "callback_data": "approval:accept"},
            {"text": "Decline", "callback_data": "approval:decline"},
            {"text": "Details", "callback_data": "approval:details"},
        ]
        if allow_always:
            keyboard_buttons.append({"text": "Always allow this", "callback_data": "approval:always_allow"})
        pending = {
            "request": event.request,
            "chat_id": event.chat_id,
            "thread_id": event.thread_id,
            "prompt_state": AccessPointDeliveryState.PENDING,
            "prompt_receipt_id": None,
            "prompt_message_id": None,
        }
        pending["prompt_receipt_id"] = self._enqueue_send_text(
            chat_id=event.chat_id,
            thread_id=event.thread_id,
            text=_format_approval_prompt(event.request),
            kind=TelegramKind.APPROVAL_PROMPT,
            reply_markup={"inline_keyboard": [keyboard_buttons]},
            parse_mode="HTML",
            priority=ACCESS_POINT_OUTBOUND_CLASS_SEND,
            on_success=lambda send_result, holder=pending, req=event.request: self._on_approval_prompt_sent(
                pending=holder,
                send_result=send_result,
                req_id=req.req_id,
            ),
            on_failure=lambda exc, holder=pending, req=event.request: self._on_approval_prompt_failed(
                pending=holder,
                exc=exc,
                req_id=req.req_id,
            ),
        )
        return pending

    def send_outbound_note(self, note: "_TelegramOutboundNote") -> None:
        self._enqueue_send_text(
            chat_id=note.chat_id,
            thread_id=note.thread_id,
            text=note.text,
            kind=TelegramKind.APPROVAL_ACK,
        )

    def access_point_for_status_event(self, event: "_TelegramStatusEvent") -> tuple[int, int | None]:
        return (event.chat_id, event.thread_id)

    def status_text_for_event(self, event: "_TelegramStatusEvent") -> str:
        return event.status_text

    def build_status_runtime(
        self,
        access_point: tuple[int, int | None],
    ) -> tuple[TelegramOutputRuntime, TurnStatusStore]:
        chat_id, thread_id = access_point
        output_runtime = TelegramOutputRuntime(
            client=self._core._client,
            logger=self._core._logger,
            source="agent",
            chat_id_getter=lambda cid=chat_id: cid,
            send_kwargs_getter=((lambda tid=thread_id: {"message_thread_id": tid}) if thread_id is not None else None),
            status_config=self._core._status_config,
        )
        status_store = TurnStatusStore(output_runtime=output_runtime)
        return (output_runtime, status_store)

    def handle_text_update(
        self,
        update: "TelegramTextUpdate",
    ) -> ProxySubmitPrompt[tuple[int, int | None]] | ProxyApprovalDecision[tuple[int, int | None]] | None:
        approval_action = self._handle_pending_approval_text(update)
        if approval_action is not None:
            return approval_action
        cmd = _normalize_command(update.text)
        if cmd in ("/start", "/status", "/stop", "/quit"):
            if cmd == "/start":
                reply = "ORC1 telegram-agent ready. Send text to proxy to local agent. Commands: /start /status /compact /model /stop"
            elif cmd == "/status":
                current_model = self._core._driver.get_actual_thread_model().strip()
                if current_model:
                    reply = f"status: running (mode=telegram-agent, allowlist=on, model={current_model})"
                else:
                    reply = "status: running (mode=telegram-agent, allowlist=on)"
            else:
                reply = "stopped"
            self._enqueue_send_text(
                chat_id=update.chat_id,
                thread_id=update.thread_id,
                text=reply,
                kind=TelegramKind.COMMAND,
            )
            self._core._writer(
                f"chat_id={update.chat_id} thread_id={update.thread_id} in={update.text!r} out={reply!r}"
            )
            return None

        if cmd == "/model":
            current_model = self._core._driver.get_actual_thread_model().strip()
            self._core._logger.event(
                "model_picker_opened",
                source="telegram_agent",
                chat_id=update.chat_id,
                thread_id=update.thread_id,
                current_model=current_model,
            )
            self._enqueue_send_text(
                chat_id=update.chat_id,
                thread_id=update.thread_id,
                text=_build_model_picker_text(current_model=current_model),
                kind=TelegramKind.COMMAND,
                reply_markup=_build_model_picker_keyboard(),
            )
            return None

        if not self._core.is_awaiting():
            warning = _format_not_waiting_warning(text=update.text)
            self._enqueue_send_text(
                chat_id=update.chat_id,
                thread_id=update.thread_id,
                text=warning,
                kind=TelegramKind.WARNING,
            )
            self._core._logger.event(
                "human_input_rejected",
                source="telegram",
                reason="not_waiting",
                chat_id=update.chat_id,
                thread_id=update.thread_id,
                text=update.text,
            )
            self._core._logger.event(
                "human_input_rejected_warning_emitted",
                source="telegram",
                reason="not_waiting",
                chat_id=update.chat_id,
                thread_id=update.thread_id,
                warning=warning,
            )
            self._core._writer(
                f"chat_id={update.chat_id} thread_id={update.thread_id} rejected not_waiting text={update.text!r}"
            )
            return None

        if cmd == "/compact":
            self._core._logger.event(
                "compact_requested",
                source="telegram_agent",
                chat_id=update.chat_id,
                thread_id=update.thread_id,
                via="command",
            )
            self._core._logger.event(
                "compact_result",
                source="telegram_agent",
                chat_id=update.chat_id,
                thread_id=update.thread_id,
                result="accepted",
            )

        self._core.set_awaiting(False)
        self._core._logger.event(
            "human_input_accepted",
            source="telegram",
            chat_id=update.chat_id,
            thread_id=update.thread_id,
            text=update.text,
        )
        return ProxySubmitPrompt(access_point=(update.chat_id, update.thread_id), text=update.text)

    def handle_callback_update(
        self,
        update: "TelegramCallbackUpdate",
    ) -> ProxyApprovalDecision[tuple[int, int | None]] | ProxyModelSelection[tuple[int, int | None]] | None:
        approval_decision = self._handle_pending_approval_callback(update)
        if approval_decision is not None:
            return approval_decision
        selected_model = _parse_model_callback(update.data)
        if selected_model is None:
            return None
        if not self._core.is_awaiting():
            self._core._logger.event(
                "model_selected",
                source="telegram_agent",
                chat_id=update.chat_id,
                thread_id=update.thread_id,
                model=selected_model,
                result="rejected_busy",
            )
            if isinstance(update.callback_query_id, str):
                self._enqueue_answer_callback_query(
                    callback_query_id=update.callback_query_id,
                    text="agent is busy",
                    priority=ACCESS_POINT_OUTBOUND_CLASS_CALLBACK_ACK,
                )
            return None
        self._core._logger.event(
            "model_selected",
            source="telegram_agent",
            chat_id=update.chat_id,
            thread_id=update.thread_id,
            model=selected_model,
        )
        if isinstance(update.callback_query_id, str):
            self._enqueue_answer_callback_query(
                callback_query_id=update.callback_query_id,
                text=f"selected: {selected_model}",
                priority=ACCESS_POINT_OUTBOUND_CLASS_CALLBACK_ACK,
            )
        if isinstance(update.message_id, int):
            self._enqueue_edit_reply_markup(
                chat_id=update.chat_id,
                message_id=update.message_id,
                reply_markup={"inline_keyboard": []},
                priority=ACCESS_POINT_OUTBOUND_CLASS_EDIT,
                on_failure=lambda exc, chat_id=update.chat_id, message_id=update.message_id: self._core._logger.event(
                    "model_picker_markup_clear_error",
                    source="telegram_agent",
                    chat_id=chat_id,
                    message_id=message_id,
                    error=str(exc),
                    error_type=type(exc).__name__,
                ),
            )
        self._core.set_awaiting(False)
        return ProxyModelSelection(access_point=(update.chat_id, update.thread_id), selected_model=selected_model)

    def _handle_pending_approval_text(
        self,
        update: "TelegramTextUpdate",
    ) -> ProxyApprovalDecision[tuple[int, int | None]] | None:
        pending = self._core.pending_approval
        if pending is None:
            return None
        if update.chat_id != pending["chat_id"]:
            return None
        if pending["thread_id"] is not None and update.thread_id != pending["thread_id"]:
            return None
        raw = update.text.strip().lower()
        if raw not in ("accept", "decline"):
            self._enqueue_send_text(
                chat_id=update.chat_id,
                thread_id=update.thread_id,
                text="reply with accept or decline",
                kind=TelegramKind.APPROVAL_INVALID,
            )
            self._core._writer(f"chat_id={update.chat_id} invalid approval response={update.text!r}")
            return ProxyApprovalDecision(access_point=(update.chat_id, update.thread_id), decision="details")
        self._split_and_clear_approval(raw=raw, via="text")
        return ProxyApprovalDecision(access_point=(update.chat_id, update.thread_id), decision=raw)

    def _handle_pending_approval_callback(
        self,
        update: "TelegramCallbackUpdate",
    ) -> ProxyApprovalDecision[tuple[int, int | None]] | None:
        pending = self._core.pending_approval
        if pending is None:
            return None
        if update.chat_id != pending["chat_id"]:
            return None
        if pending["thread_id"] is not None and update.thread_id != pending["thread_id"]:
            return None
        if pending.get("prompt_state") != AccessPointDeliveryState.SENT:
            return None
        prompt_message_id = pending.get("prompt_message_id")
        if isinstance(prompt_message_id, int) and update.message_id != prompt_message_id:
            return None
        if update.data == "approval:details":
            self._enqueue_send_text(
                chat_id=update.chat_id,
                thread_id=update.thread_id,
                text=_format_approval_details(pending["request"]),
                kind=TelegramKind.APPROVAL_DETAILS,
            )
            if isinstance(update.callback_query_id, str):
                self._enqueue_answer_callback_query(
                    callback_query_id=update.callback_query_id,
                    text="details sent",
                    priority=ACCESS_POINT_OUTBOUND_CLASS_CALLBACK_ACK,
                )
            return ProxyApprovalDecision(access_point=(update.chat_id, update.thread_id), decision="details")
        if update.data not in ("approval:accept", "approval:decline", "approval:always_allow"):
            return None
        decision = update.data.split(":", 1)[1]
        resolved = "always_allow" if decision == "always_allow" else decision
        if isinstance(update.callback_query_id, str):
            self._enqueue_answer_callback_query(
                callback_query_id=update.callback_query_id,
                text=f"selected: {decision}",
                priority=ACCESS_POINT_OUTBOUND_CLASS_CALLBACK_ACK,
            )
        self._split_and_clear_approval(raw=resolved, via="button")
        if decision == "always_allow":
            self._enqueue_send_text(
                chat_id=update.chat_id,
                thread_id=update.thread_id,
                text="Applying: accept with acceptSettings (always-allow), if supported by app-server.",
                kind=TelegramKind.APPROVAL_ACK,
            )
        return ProxyApprovalDecision(access_point=(update.chat_id, update.thread_id), decision=resolved)

    def _split_and_clear_approval(self, *, raw: str, via: str) -> None:
        pending = self._core.pending_approval
        if pending is None:
            return
        self._cancel_pending_approval_prompt_if_needed(pending)
        prompt_message_id = pending.get("prompt_message_id")
        if isinstance(prompt_message_id, int):
            self._enqueue_edit_reply_markup(
                chat_id=pending["chat_id"],
                message_id=prompt_message_id,
                reply_markup={"inline_keyboard": []},
                priority=ACCESS_POINT_OUTBOUND_CLASS_APPROVAL_CLEANUP,
                on_failure=lambda exc, pending=pending, prompt_message_id=prompt_message_id: self._core._logger.event(
                    "telegram_approval_markup_clear_error",
                    chat_id=pending["chat_id"],
                    thread_id=pending["thread_id"],
                    message_id=prompt_message_id,
                    error=str(exc),
                    error_type=type(exc).__name__,
                ),
            )
        self._core.split_status_after_approval(access_point=(pending["chat_id"], pending["thread_id"]))
        self._core._logger.event(
            "telegram_approval_human_response",
            chat_id=pending["chat_id"],
            thread_id=pending["thread_id"],
            id=pending["request"].req_id,
            response=raw,
            via=via,
        )
        self._core.clear_pending_approval()

    def flush_delivery_lane(self) -> bool:
        return self._drain_outbound_queue()

    def _on_approval_prompt_sent(
        self,
        *,
        pending: dict[str, Any],
        send_result: _TelegramProxyOutboundResult,
        req_id: str,
    ) -> None:
        pending["prompt_state"] = AccessPointDeliveryState.SENT
        pending["prompt_receipt_id"] = None
        pending["prompt_message_id"] = send_result.first_message_id
        self._core._logger.event(
            "telegram_approval_prompt_sent",
            chat_id=pending["chat_id"],
            thread_id=pending["thread_id"],
            id=req_id,
            message_id=send_result.first_message_id,
        )

    def _on_approval_prompt_failed(
        self,
        *,
        pending: dict[str, Any],
        exc: Exception,
        req_id: str,
    ) -> None:
        pending["prompt_state"] = AccessPointDeliveryState.FAILED
        pending["prompt_receipt_id"] = None
        pending["prompt_message_id"] = None
        self._core._logger.event(
            "telegram_approval_prompt_error",
            chat_id=pending["chat_id"],
            thread_id=pending["thread_id"],
            id=req_id,
            error=str(exc),
            error_type=type(exc).__name__,
        )

    def _cancel_pending_approval_prompt_if_needed(self, pending: dict[str, Any]) -> None:
        receipt_id = pending.get("prompt_receipt_id")
        if not isinstance(receipt_id, int):
            return
        for item in list(self._outbound_queue):
            if item.receipt_id != receipt_id:
                continue
            self._finish_outbound_item(item)
            pending["prompt_receipt_id"] = None
            pending["prompt_state"] = AccessPointDeliveryState.CANCELLED
            receipt = self._receipts.get(receipt_id)
            if receipt is not None:
                receipt.mark_cancelled()
            self._core._logger.event(
                "telegram_approval_prompt_cancelled",
                chat_id=pending["chat_id"],
                thread_id=pending["thread_id"],
                id=pending["request"].req_id,
            )
            return

    def _command_runtime(self, chat_id: int, thread_id: int | None) -> TelegramOutputRuntime:
        return self._core._command_runtime(chat_id, thread_id)

    def _enqueue_send_text(
        self,
        *,
        chat_id: int,
        thread_id: int | None,
        text: str,
        kind: TelegramKind,
        reply_markup: dict | None = None,
        parse_mode: str | None = None,
        priority: int = ACCESS_POINT_OUTBOUND_CLASS_SEND,
        on_success: Any = None,
        on_failure: Any = None,
    ) -> int:
        return self._enqueue_outbound(
            operation=f"send_text:{kind}",
            priority=priority,
            execute=lambda cid=chat_id, tid=thread_id, body=text, body_kind=kind, markup=reply_markup, mode=parse_mode: self._execute_send_text(
                chat_id=cid,
                thread_id=tid,
                text=body,
                kind=body_kind,
                reply_markup=markup,
                parse_mode=mode,
            ),
            on_success=on_success,
            on_failure=on_failure,
        )

    def _execute_send_text(
        self,
        *,
        chat_id: int,
        thread_id: int | None,
        text: str,
        kind: TelegramKind,
        reply_markup: dict | None,
        parse_mode: str | None,
    ) -> _TelegramProxyOutboundResult:
        result = self._command_runtime(chat_id, thread_id).send_text(
            text=text,
            kind=kind,
            chat_id=chat_id,
            reply_markup=reply_markup,
            parse_mode=parse_mode,
            raise_on_error=True,
        )
        return _TelegramProxyOutboundResult(
            sent=result.sent_chunks > 0,
            first_message_id=result.first_message_id,
            sent_chunks=result.sent_chunks,
        )

    def _enqueue_edit_reply_markup(
        self,
        *,
        chat_id: int,
        message_id: int,
        reply_markup: dict | None,
        priority: int = ACCESS_POINT_OUTBOUND_CLASS_EDIT,
        on_success: Any = None,
        on_failure: Any = None,
    ) -> int:
        return self._enqueue_outbound(
            operation="edit_reply_markup",
            priority=priority,
            coalesce_key=("edit_reply_markup", int(chat_id), int(message_id)),
            execute=lambda cid=chat_id, mid=message_id, markup=reply_markup: self._execute_edit_reply_markup(
                chat_id=cid,
                message_id=mid,
                reply_markup=markup,
            ),
            on_success=on_success,
            on_failure=on_failure,
        )

    def _execute_edit_reply_markup(
        self,
        *,
        chat_id: int,
        message_id: int,
        reply_markup: dict | None,
    ) -> _TelegramProxyOutboundResult:
        self._core._client.edit_message_reply_markup(
            chat_id=chat_id,
            message_id=message_id,
            reply_markup=reply_markup,
        )
        return _TelegramProxyOutboundResult(sent=True)

    def _enqueue_answer_callback_query(
        self,
        *,
        callback_query_id: str,
        text: str | None,
        priority: int = ACCESS_POINT_OUTBOUND_CLASS_CALLBACK_ACK,
        on_success: Any = None,
        on_failure: Any = None,
    ) -> int:
        return self._enqueue_outbound(
            operation="answer_callback_query",
            priority=priority,
            execute=lambda cbid=callback_query_id, body=text: self._execute_answer_callback_query(
                callback_query_id=cbid,
                text=body,
            ),
            on_success=on_success,
            on_failure=on_failure,
        )

    def _execute_answer_callback_query(
        self,
        *,
        callback_query_id: str,
        text: str | None,
    ) -> _TelegramProxyOutboundResult:
        self._core._client.answer_callback_query(callback_query_id, text=text)
        return _TelegramProxyOutboundResult(sent=True)

    def _enqueue_outbound(
        self,
        *,
        operation: str,
        priority: int,
        execute: Any,
        on_success: Any = None,
        on_failure: Any = None,
        coalesce_key: tuple[Any, ...] | None = None,
    ) -> int:
        if coalesce_key is not None and coalesce_key in self._queued_keys:
            return 0
        receipt_id = self._next_outbound_receipt_id
        self._next_outbound_receipt_id += 1
        self._receipts[receipt_id] = AccessPointDeliveryReceipt(receipt_id=receipt_id)
        self._outbound_queue.append(
            QueuedAccessPointOutbound(
                receipt_id=receipt_id,
                priority=int(priority),
                sequence=self._next_outbound_sequence,
                operation=operation,
                coalesce_key=coalesce_key,
                progress_on_success=True,
                execute=execute,
                on_success=on_success,
                on_failure=on_failure,
            )
        )
        self._next_outbound_sequence += 1
        self._outbound_queue.sort(key=access_point_outbound_sort_key)
        if coalesce_key is not None:
            self._queued_keys.add(coalesce_key)
        return receipt_id

    def _drain_outbound_queue(self) -> bool:
        if not self._outbound_queue:
            return False
        item = self._outbound_queue[0]
        try:
            result = item.execute()
        except Exception as exc:
            receipt = self._receipts.get(item.receipt_id)
            if receipt is not None:
                receipt.mark_failed(exc)
            self._finish_outbound_item(item)
            if callable(item.on_failure):
                item.on_failure(exc)
            return True
        receipt = self._receipts.get(item.receipt_id)
        if receipt is not None:
            receipt.mark_sent(message_id=result.first_message_id)
        self._finish_outbound_item(item)
        if callable(item.on_success):
            item.on_success(result)
        return result.sent

    def _finish_outbound_item(self, item: QueuedAccessPointOutbound[_TelegramProxyOutboundResult]) -> None:
        if self._outbound_queue and self._outbound_queue[0] is item:
            self._outbound_queue.pop(0)
        else:
            try:
                self._outbound_queue.remove(item)
            except ValueError:
                pass
        if item.coalesce_key is not None:
            self._queued_keys.discard(item.coalesce_key)


def _truncate_echo_text(text: str, limit: int = 3500) -> str:
    if len(text) <= limit:
        return text
    return text[: max(0, limit - 3)] + "..."


def _format_not_waiting_warning(text: str) -> str:
    echoed = _truncate_echo_text(text)
    return (
        "[WARN] input rejected: agent is busy, resend after current reply.\n"
        "Echo:\n"
        f"{echoed}"
    )


def _normalize_command(text: str) -> str | None:
    token = str(text or "").strip().split(maxsplit=1)[0].lower()
    if not token.startswith("/"):
        return None
    if "@" in token:
        token = token.split("@", 1)[0]
    return token


def _build_model_picker_text(*, current_model: str) -> str:
    if current_model:
        return f"Select model for this access point.\nCurrent: {current_model}"
    return "Select model for this access point."


def _build_model_picker_keyboard() -> dict:
    from orchestrator.proxy_model_choices import SUPPORTED_MODEL_CHOICES

    buttons = [
        {"text": model, "callback_data": f"model:set:{model}"}
        for model in SUPPORTED_MODEL_CHOICES
    ]
    return {"inline_keyboard": [buttons]}


def _parse_model_callback(data: str) -> str | None:
    from orchestrator.proxy_model_choices import SUPPORTED_MODEL_CHOICES

    prefix = "model:set:"
    if not isinstance(data, str) or not data.startswith(prefix):
        return None
    model = data[len(prefix) :].strip()
    if model not in SUPPORTED_MODEL_CHOICES:
        return None
    return model
