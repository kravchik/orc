from __future__ import annotations

import threading
from typing import Callable

from orchestrator.access_point_common import (
    ACCESS_POINT_OUTBOUND_CLASS_APPROVAL_CLEANUP,
    AccessPointDeliveryState,
)
from orchestrator.approval import ApprovalDecisionProvider, ApprovalRequest, build_accept_settings
from orchestrator.processes import LifecycleLogger
from orchestrator.telegram_bridge import TelegramCallbackUpdate, TelegramTextUpdate
from orchestrator.telegram_delivery_lane import TelegramDeliveryLane
from orchestrator.telegram_output_runtime import TelegramKind, TelegramOutputRuntime, TelegramSendResult
from orchestrator.telegram_smoke import TelegramApi


Writer = Callable[[str], None]


def _shorten(text: str, limit: int = 120) -> str:
    compact = " ".join(str(text).split())
    if len(compact) <= limit:
        return compact
    return compact[: limit - 3] + "..."


def _approval_source_tag(request: ApprovalRequest) -> str:
    source = str(getattr(request, "role", "") or "").strip().upper()
    if source:
        return source
    return "AGENT"


def _format_approval_prompt(request: ApprovalRequest) -> str:
    params = request.params
    command = params.get("command")
    cwd = params.get("cwd")
    lines = [f"[{_approval_source_tag(request)}->HUMAN] approval needed"]
    if isinstance(command, str) and command.strip():
        lines.append(f"command: {_shorten(command, limit=220)}")
    if isinstance(cwd, str) and cwd.strip():
        lines.append(f"cwd: {cwd}")
    return "\n".join(lines)


def _format_approval_details(request: ApprovalRequest) -> str:
    params = request.params
    command = params.get("command")
    cwd = params.get("cwd")
    reason = params.get("reason")
    actions = params.get("commandActions")
    lines = [
        f"[{_approval_source_tag(request)}->HUMAN] approval details",
        f"method={request.method}",
        f"role={getattr(request, 'role', '')}",
    ]
    if isinstance(command, str):
        lines.append(f"command={command}")
    if isinstance(cwd, str):
        lines.append(f"cwd={cwd}")
    if reason is not None:
        lines.append(f"reason={reason}")
    if actions is not None:
        lines.append(f"actions={actions}")
    return "\n".join(lines)


class TelegramApprovalDecisionProvider(ApprovalDecisionProvider):
    def __init__(
        self,
        client: TelegramApi,
        logger: LifecycleLogger,
        allowed_chat_ids: set[int],
        offset_ref: list[int | None],
        poll_timeout_sec: int,
        writer: Writer,
        allow_any_current_chat: bool = False,
        use_external_poll_loop: bool = False,
        on_decision_resolved: Callable[[int, int | None, str], None] | None = None,
    ) -> None:
        self._client = client
        self._logger = logger
        self._allowed_chat_ids = allowed_chat_ids
        self._offset_ref = offset_ref
        self._poll_timeout_sec = poll_timeout_sec
        self._writer = writer
        self._current_chat_id: int | None = None
        self._current_thread_id: int | None = None
        self._allow_any_current_chat = bool(allow_any_current_chat)
        self._use_external_poll_loop = bool(use_external_poll_loop)
        self._on_decision_resolved = on_decision_resolved
        self._pending_lock = threading.Lock()
        self._pending_cv = threading.Condition(self._pending_lock)
        self._pending_request: dict | None = None
        self._delivery = TelegramDeliveryLane(logger=logger, source="telegram_agent")

    def notify_auto_approval(self, text: str) -> None:
        target_chat_id = self._current_chat_id
        if target_chat_id is None:
            return
            self._send_text(
                chat_id=target_chat_id,
                thread_id=self._current_thread_id,
            text=text,
            kind=TelegramKind.APPROVAL_AUTO,
            raise_on_error=True,
        )

    def set_current_chat(self, chat_id: int, thread_id: int | None = None) -> None:
        self._current_chat_id = chat_id
        self._current_thread_id = thread_id
        if self._allow_any_current_chat:
            self._allowed_chat_ids.add(chat_id)

    def set_on_decision_resolved(
        self,
        callback: Callable[[int, int | None, str], None] | None,
    ) -> None:
        self._on_decision_resolved = callback

    def decide(self, request: ApprovalRequest) -> str:
        target_chat_id = self._current_chat_id
        target_thread_id = self._current_thread_id
        if target_chat_id is None:
            raise RuntimeError("no chat context available for human approval")
        if not self._allow_any_current_chat and target_chat_id not in self._allowed_chat_ids:
            raise RuntimeError("no chat context available for human approval")

        prompt = _format_approval_prompt(request)
        keyboard_buttons = [
            {"text": "Accept", "callback_data": "approval:accept"},
            {"text": "Decline", "callback_data": "approval:decline"},
            {"text": "Details", "callback_data": "approval:details"},
        ]
        if build_accept_settings(params=request.params) is not None:
            keyboard_buttons.append({"text": "Always allow this", "callback_data": "approval:always_allow"})
        keyboard = {"inline_keyboard": [keyboard_buttons]}
        with self._pending_cv:
            self._pending_request = {
                "request": request,
                "chat_id": target_chat_id,
                "thread_id": target_thread_id,
                "prompt_message_id": None,
                "markup_cleared": False,
                "response": None,
            }
        try:
            prompt_result = self._send_text(
                chat_id=target_chat_id,
                thread_id=target_thread_id,
                text=prompt,
                kind=TelegramKind.APPROVAL_PROMPT,
                reply_markup=keyboard,
                parse_mode="HTML",
                raise_on_error=True,
            )
        except Exception:
            with self._pending_cv:
                self._pending_request = None
                self._pending_cv.notify_all()
            raise
        prompt_message_id = prompt_result.first_message_id
        with self._pending_cv:
            if self._pending_request is not None:
                self._pending_request["prompt_message_id"] = prompt_message_id
                response = self._pending_request.get("response")
                markup_cleared = bool(self._pending_request.get("markup_cleared"))
            else:
                response = None
                markup_cleared = False
        if isinstance(prompt_message_id, int) and isinstance(response, str) and not markup_cleared:
            self._clear_approval_markup(
                chat_id=target_chat_id,
                thread_id=target_thread_id,
                message_id=prompt_message_id,
            )
            with self._pending_cv:
                if self._pending_request is not None:
                    self._pending_request["markup_cleared"] = True
        self._logger.event("telegram_approval_prompt_sent", chat_id=target_chat_id, id=request.req_id)
        if not self._use_external_poll_loop:
            self._drain_updates_until_resolved()
        with self._pending_cv:
            while True:
                response = self._pending_request.get("response")
                if isinstance(response, str):
                    self._pending_request = None
                    return response
                self._pending_cv.wait(timeout=max(0.2, float(self._poll_timeout_sec)))

    def _drain_updates_until_resolved(self) -> None:
        while True:
            with self._pending_cv:
                pending = self._pending_request
                if pending is None:
                    return
                if isinstance(pending.get("response"), str):
                    return
            updates = self._client.get_updates(offset=self._offset_ref[0], timeout_sec=self._poll_timeout_sec)
            for upd in updates:
                upd_id = upd.get("update_id")
                if isinstance(upd_id, int):
                    self._offset_ref[0] = upd_id + 1
                callback = upd.get("callback_query")
                if isinstance(callback, dict):
                    msg = callback.get("message")
                    cb_chat_id: int | None = None
                    cb_message_id: int | None = None
                    cb_thread_id: int | None = None
                    if isinstance(msg, dict):
                        chat = msg.get("chat")
                        if isinstance(chat, dict):
                            cid = chat.get("id")
                            if isinstance(cid, int):
                                cb_chat_id = cid
                        mid = msg.get("message_id")
                        if isinstance(mid, int):
                            cb_message_id = mid
                        tid = msg.get("message_thread_id")
                        if isinstance(tid, int):
                            cb_thread_id = tid
                    data = callback.get("data")
                    cb_id = callback.get("id")
                    if isinstance(cb_chat_id, int) and isinstance(data, str):
                        self.handle_callback_update(
                            TelegramCallbackUpdate(
                                update_id=upd_id if isinstance(upd_id, int) else None,
                                chat_id=cb_chat_id,
                                message_id=cb_message_id,
                                thread_id=cb_thread_id,
                                callback_query_id=cb_id if isinstance(cb_id, str) else None,
                                data=data,
                            )
                        )
                    continue
                message = upd.get("message")
                if not isinstance(message, dict):
                    continue
                chat = message.get("chat")
                text = message.get("text")
                if not isinstance(chat, dict) or not isinstance(text, str):
                    continue
                chat_id = chat.get("id")
                if not isinstance(chat_id, int):
                    continue
                message_id_raw = message.get("message_id")
                message_id = message_id_raw if isinstance(message_id_raw, int) else None
                thread_id_raw = message.get("message_thread_id")
                thread_id = thread_id_raw if isinstance(thread_id_raw, int) else None
                self.handle_text_update(
                    TelegramTextUpdate(
                        update_id=upd_id if isinstance(upd_id, int) else None,
                        chat_id=chat_id,
                        message_id=message_id,
                        thread_id=thread_id,
                        text=text,
                    )
                )

    def handle_callback_update(self, update: TelegramCallbackUpdate) -> bool:
        with self._pending_cv:
            pending = self._pending_request
            if pending is None:
                return False
            if update.chat_id != pending["chat_id"]:
                return False
            prompt_message_id = pending.get("prompt_message_id")
            if (
                isinstance(prompt_message_id, int)
                and isinstance(update.message_id, int)
                and update.message_id != prompt_message_id
            ):
                return False
            data = update.data
            request = pending["request"]
            target_chat_id = pending["chat_id"]
            target_thread_id = pending["thread_id"]

        if data == "approval:details":
            self._send_text(
                chat_id=target_chat_id,
                thread_id=target_thread_id,
                text=_format_approval_details(request),
                kind=TelegramKind.APPROVAL_DETAILS,
                raise_on_error=True,
            )
            if isinstance(update.callback_query_id, str):
                self._answer_callback_query(update.callback_query_id, text="details sent")
            return True

        if data not in ("approval:accept", "approval:decline", "approval:always_allow"):
            return False

        decision = data.split(":", 1)[1]
        response = "always_allow" if decision == "always_allow" else decision
        if callable(self._on_decision_resolved):
            self._on_decision_resolved(target_chat_id, target_thread_id, response)
        if isinstance(update.callback_query_id, str):
            self._answer_callback_query(update.callback_query_id, text=f"selected: {decision}")
        if isinstance(prompt_message_id, int):
            self._clear_approval_markup(
                chat_id=target_chat_id,
                thread_id=target_thread_id,
                message_id=prompt_message_id,
            )
            with self._pending_cv:
                if self._pending_request is not None:
                    self._pending_request["markup_cleared"] = True
        if decision == "always_allow":
            self._send_text(
                chat_id=target_chat_id,
                thread_id=target_thread_id,
                text="Applying: accept with acceptSettings (always-allow), if supported by app-server.",
                kind=TelegramKind.APPROVAL_ACK,
                raise_on_error=True,
            )
        self._logger.event(
            "telegram_approval_human_response",
            chat_id=target_chat_id,
            thread_id=target_thread_id,
            id=request.req_id,
            response=response,
            via="button",
            button=decision,
        )
        with self._pending_cv:
            if self._pending_request is None:
                return True
            self._pending_request["response"] = response
            self._pending_cv.notify_all()
        return True

    def handle_text_update(self, update: TelegramTextUpdate) -> bool:
        with self._pending_cv:
            pending = self._pending_request
            if pending is None:
                return False
            target_chat_id = pending["chat_id"]
            target_thread_id = pending["thread_id"]
            prompt_message_id = pending.get("prompt_message_id")
            request = pending["request"]
            if update.chat_id != target_chat_id:
                return False
            if target_thread_id is not None and update.thread_id != target_thread_id:
                return False
            raw = update.text.strip().lower()
            if raw in ("accept", "decline"):
                if callable(self._on_decision_resolved):
                    self._on_decision_resolved(target_chat_id, target_thread_id, raw)
                if isinstance(prompt_message_id, int):
                    self._clear_approval_markup(
                        chat_id=target_chat_id,
                        thread_id=target_thread_id,
                        message_id=prompt_message_id,
                    )
                    self._pending_request["markup_cleared"] = True
                self._logger.event(
                    "telegram_approval_human_response",
                    chat_id=target_chat_id,
                    thread_id=target_thread_id,
                    id=request.req_id,
                    response=raw,
                    via="text",
                )
                self._pending_request["response"] = raw
                self._pending_cv.notify_all()
                return True

        self._send_text(
            chat_id=update.chat_id,
            thread_id=update.thread_id,
            text="reply with accept or decline",
            kind=TelegramKind.APPROVAL_INVALID,
            raise_on_error=True,
        )
        self._writer(f"chat_id={update.chat_id} invalid approval response={update.text!r}")
        return True

    def _clear_approval_markup(self, *, chat_id: int, thread_id: int | None, message_id: int) -> None:
        failure: dict[str, Exception] = {}
        receipt_id = self._delivery.enqueue_edit_message_reply_markup(
            client=self._client,
            chat_id=chat_id,
            message_id=message_id,
            reply_markup={"inline_keyboard": []},
            priority=ACCESS_POINT_OUTBOUND_CLASS_APPROVAL_CLEANUP,
            on_failure=lambda exc: failure.setdefault("exc", exc),
        )
        try:
            self._delivery.wait(receipt_id)
        except Exception as exc:
            exc = failure.get("exc", exc)
            self._logger.event(
                "telegram_approval_markup_clear_error",
                chat_id=chat_id,
                thread_id=thread_id,
                message_id=message_id,
                error=str(exc),
                error_type=type(exc).__name__,
            )

    def _answer_callback_query(self, callback_query_id: str, *, text: str | None = None) -> None:
        failure: dict[str, Exception] = {}
        receipt_id = self._delivery.enqueue_answer_callback_query(
            client=self._client,
            callback_query_id=callback_query_id,
            text=text,
            on_failure=lambda exc: failure.setdefault("exc", exc),
        )
        receipt = self._delivery.wait(receipt_id)
        if receipt.state == AccessPointDeliveryState.FAILED:
            raise failure.get("exc", RuntimeError(receipt.error or "telegram callback answer failed"))

    def _send_text(
        self,
        *,
        chat_id: int,
        thread_id: int | None,
        text: str,
        kind: str,
        reply_markup: dict | None = None,
        parse_mode: str | None = None,
        raise_on_error: bool = False,
    ):
        runtime = TelegramOutputRuntime(
            client=self._client,
            logger=self._logger,
            source="telegram_agent",
            chat_id_getter=lambda: chat_id,
            send_kwargs_getter=(
                (lambda tid=thread_id: {"message_thread_id": tid}) if thread_id is not None else None
            ),
        )
        failure: dict[str, Exception] = {}
        receipt_id = self._delivery.enqueue_send_text(
            output_runtime=runtime,
            chat_id=chat_id,
            text=text,
            kind=kind,
            reply_markup=reply_markup,
            parse_mode=parse_mode,
            raise_on_error=raise_on_error,
            on_failure=lambda exc: failure.setdefault("exc", exc),
        )
        receipt = self._delivery.wait(receipt_id)
        if receipt.state == AccessPointDeliveryState.FAILED and raise_on_error:
            raise failure.get("exc", RuntimeError(receipt.error or "telegram delivery failed"))
        return TelegramSendResult(
            chat_id=chat_id,
            first_message_id=receipt.message_id,
            sent_chunks=1 if receipt.message_id is not None else 0,
        )
