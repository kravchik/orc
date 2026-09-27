"""Telegram access-point adapter helpers for steward runtime."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable

from orchestrator import clock
from orchestrator.access_point_common import (
    ACCESS_POINT_OUTBOUND_CLASS_APPROVAL_CLEANUP,
    ACCESS_POINT_OUTBOUND_CLASS_APPROVAL_NOTICE,
    ACCESS_POINT_OUTBOUND_CLASS_CALLBACK_ACK,
    ACCESS_POINT_OUTBOUND_CLASS_EDIT,
    ACCESS_POINT_OUTBOUND_CLASS_LIFECYCLE_UPDATE,
    ACCESS_POINT_OUTBOUND_CLASS_SEND,
    AccessPointKey,
    AccessPointMessageRef,
    AccessPointDeliveryState,
    AccessPointOutboundQueue,
    AccessPointRetryPolicy,
    QueuedAccessPointOutbound,
    access_point_delivery_log_fields,
    decide_access_point_outbound_failure,
    expire_access_point_outbounds,
    format_access_point,
    log_abandoned_access_point_outbounds,
    select_access_point_outbound,
    wake_durable_access_point_outbounds,
)
from orchestrator.approval_details_formatter import (
    approval_source_icon,
    build_approval_details_lines,
    build_approval_prompt_detail_lines,
)
from orchestrator.approval import ApprovalRequest, supports_session_approval
from orchestrator.processes import LifecycleLogger
from orchestrator.runtime_snapshot import AccessPointRuntimeSnapshot, format_status_text
from orchestrator.steward_restore_notice_rendering import render_session_references_html
from orchestrator.steward_inbound import (
    StewardApprovalDecision,
    StewardApprovalDetailsRequest,
    StewardInboundText,
)
from orchestrator.telegram_api_thread_driver import TelegramApiThreadDriver
from orchestrator.telegram_output_runtime import (
    TelegramBotStatusBudget,
    TelegramDueStatusCandidate,
    TelegramKind,
    TelegramOutputRuntime,
)
from orchestrator.steward_commands import StewardHelpContext, build_steward_help_text
from orchestrator.telegram_status import TelegramStatusConfig
from orchestrator.turn_status_store import TurnStatusStore
from orchestrator.telegram_bridge import TelegramCallbackUpdate, TelegramTextUpdate


@dataclass
class PendingTelegramStewardApproval:
    access_point: AccessPointKey
    source: str
    request: ApprovalRequest
    prompt_state: AccessPointDeliveryState
    prompt_queue_token: int | None
    prompt_message_id: int | None


@dataclass(frozen=True)
class _TelegramOutboundExecutionResult:
    sent: bool
    first_message_id: int | None = None
    sent_chunks: int = 0


def _format_steward_approval_prompt(request: ApprovalRequest) -> str:
    header = f"{approval_source_icon(request)} approval needed"
    lines = [header]
    lines.extend(build_approval_prompt_detail_lines(request, string_limit=220))
    return "\n".join(lines)


def _format_steward_approval_details(request: ApprovalRequest) -> str:
    return "\n".join(
        build_approval_details_lines(
            request,
            header=f"{approval_source_icon(request)} approval details",
            include_role=True,
        )
    )


class TelegramStewardAccessPointAdapter:
    """Telegram-specific AP rendering and approval UI for steward runtime."""

    def __init__(
        self,
        *,
        client: TelegramApiThreadDriver,
        logger: LifecycleLogger,
        writer: Callable[[str], None],
        status_config: TelegramStatusConfig,
        status_monotonic_now: Callable[[], float] | None = None,
    ) -> None:
        self._client = client
        self._logger = logger
        self._writer = writer
        self._status_config = status_config
        self._status_monotonic_now = status_monotonic_now
        self._status_budget = TelegramBotStatusBudget(
            logger=logger,
            source="telegram_steward",
            monotonic_now=status_monotonic_now or clock.monotonic,
            cooldown_sec=2.0,
        )
        self._outbound = AccessPointOutboundQueue[_TelegramOutboundExecutionResult]()
        self._approval_prompt_failure_handler: Callable[[AccessPointKey, Exception], None] | None = None
        self._steward_status_runtime_by_access_point: dict[
            AccessPointKey,
            tuple[TelegramOutputRuntime, TurnStatusStore],
        ] = {}
        self._agent_status_runtime_by_access_point: dict[
            AccessPointKey,
            tuple[TelegramOutputRuntime, TurnStatusStore],
        ] = {}

    def set_approval_prompt_failure_handler(
        self,
        handler: Callable[[AccessPointKey, Exception], None] | None,
    ) -> None:
        self._approval_prompt_failure_handler = handler

    def status_runtime_for(
        self,
        *,
        access_point: AccessPointKey,
        source: str,
    ) -> tuple[TelegramOutputRuntime, TurnStatusStore]:
        mapping = (
            self._steward_status_runtime_by_access_point
            if source == "steward"
            else self._agent_status_runtime_by_access_point
        )
        existing = mapping.get(access_point)
        if existing is not None:
            return existing
        chat_id = int(access_point.chat_id)
        thread_id = access_point.thread_id
        output_runtime = TelegramOutputRuntime(
            client=self._client,
            logger=self._logger,
            source=source,
            chat_id_getter=lambda cid=chat_id: cid,
            send_kwargs_getter=(
                (lambda tid=thread_id: {"message_thread_id": tid})
                if thread_id is not None
                else (lambda: {})
            ),
            status_budget=self._status_budget,
            status_eager_flush_when_due=False,
            status_config=self._status_config,
            status_monotonic_now=self._status_monotonic_now,
        )
        status_store = TurnStatusStore(output_runtime=output_runtime)
        pair = (output_runtime, status_store)
        mapping[access_point] = pair
        return pair

    def clear_status_runtimes(self, *, access_point: AccessPointKey, include_steward: bool = False) -> None:
        sources = {"agent", "steward"} if include_steward else {"agent"}
        removed = self._drop_queued_status_updates(
            access_point=access_point,
            sources=sources,
        )
        runtime_entry = self._agent_status_runtime_by_access_point.get(access_point)
        if runtime_entry is not None:
            _runtime_output, runtime_store = runtime_entry
            runtime_store.clear()
        if include_steward:
            steward_entry = self._steward_status_runtime_by_access_point.get(access_point)
            if steward_entry is not None:
                _steward_output, steward_store = steward_entry
                steward_store.clear()
        if removed > 0:
            self._log_pending_status_updates_dropped(
                access_point=access_point,
                removed=removed,
            )

    def drop_pending_status_updates(
        self,
        *,
        access_point: AccessPointKey,
        include_steward: bool = False,
    ) -> None:
        sources = {"agent", "steward"} if include_steward else {"agent"}
        runtime_entry = self._agent_status_runtime_by_access_point.get(access_point)
        if runtime_entry is not None:
            runtime_output, _runtime_store = runtime_entry
            runtime_output.discard_pending_status_updates()
        if include_steward:
            steward_entry = self._steward_status_runtime_by_access_point.get(access_point)
            if steward_entry is not None:
                steward_output, _steward_store = steward_entry
                steward_output.discard_pending_status_updates()
        removed = self._drop_queued_status_updates(
            access_point=access_point,
            sources=sources,
        )
        if removed > 0:
            self._log_pending_status_updates_dropped(access_point=access_point, removed=removed)

    def flush_due_status_runtimes(self) -> bool:
        candidates: list[
            tuple[AccessPointKey, str, TelegramOutputRuntime, TelegramDueStatusCandidate]
        ] = []
        for access_point, (output_runtime, _status_store) in self._steward_status_runtime_by_access_point.items():
            candidate = output_runtime.peek_due_status_candidate()
            if candidate is not None:
                candidates.append((access_point, "steward", output_runtime, candidate))
        for access_point, (output_runtime, _status_store) in self._agent_status_runtime_by_access_point.items():
            candidate = output_runtime.peek_due_status_candidate()
            if candidate is not None:
                candidates.append((access_point, "agent", output_runtime, candidate))
        if candidates:
            selected_access_point, selected_source, selected_runtime, selected_candidate = min(
                candidates,
                key=lambda pair: (
                    float("-inf") if pair[3].last_flush_ts is None else float(pair[3].last_flush_ts),
                    pair[3].status_key,
                ),
            )
            self._enqueue_status_flush(
                access_point=selected_access_point,
                source=selected_source,
                output_runtime=selected_runtime,
                candidate=selected_candidate,
            )
        return self._drain_outbound_queue()

    def has_pending_outbound(self) -> bool:
        return bool(self._outbound)

    def log_delivery_handles_closed(self) -> None:
        log_abandoned_access_point_outbounds(
            self._outbound,
            logger=self._logger,
            source="telegram_steward",
            access_point_type="telegram",
        )
        self._logger.event(
            "telegram_delivery_handles_closed",
            source="telegram_steward",
            live_delivery_handles=self._outbound.count,
            peak_live_delivery_handles=self._outbound.peak_count,
        )

    def split_status(self, access_point: AccessPointKey) -> None:
        steward_entry = self._steward_status_runtime_by_access_point.get(access_point)
        if steward_entry is not None:
            steward_output, steward_store = steward_entry
            self._enqueue_current_turn_status_flush(
                access_point=access_point,
                source="steward",
                output_runtime=steward_output,
                status_store=steward_store,
            )
            steward_store.split_current_turn()
        agent_entry = self._agent_status_runtime_by_access_point.get(access_point)
        if agent_entry is not None:
            agent_output, agent_store = agent_entry
            self._enqueue_current_turn_status_flush(
                access_point=access_point,
                source="agent",
                output_runtime=agent_output,
                status_store=agent_store,
            )
            agent_store.split_current_turn()

    def build_help_text(self, *, state: str, startup: StewardHelpContext) -> str:
        return build_steward_help_text(state=state, startup=startup)

    def build_status_text(
        self,
        *,
        snapshot: AccessPointRuntimeSnapshot,
    ) -> str:
        access_point = snapshot.access_point
        return format_status_text(
            snapshot,
            access_point_line=f"access_point: {format_access_point(access_point)}",
        )

    def register_approval(
        self,
        *,
        access_point: AccessPointKey,
        source: str,
        request: ApprovalRequest,
    ) -> PendingTelegramStewardApproval:
        allow_always = supports_session_approval(
            method=request.method,
            params=request.params,
        )
        buttons = [
            {"text": "Accept", "callback_data": "approval:accept"},
            {"text": "Decline", "callback_data": "approval:decline"},
            {"text": "Details", "callback_data": "approval:details"},
        ]
        if allow_always:
            buttons.append({"text": "Always allow this", "callback_data": "approval:always_allow"})
        output_runtime, _status_store = self.status_runtime_for(
            access_point=access_point,
            source="agent" if source == "agent" else "steward",
        )
        pending = PendingTelegramStewardApproval(
            access_point=access_point,
            source=source,
            request=request,
            prompt_state=AccessPointDeliveryState.PENDING,
            prompt_queue_token=None,
            prompt_message_id=None,
        )
        pending.prompt_queue_token = self._enqueue_send_text(
            output_runtime=output_runtime,
            text=_format_steward_approval_prompt(request),
            kind=TelegramKind.APPROVAL_PROMPT,
            chat_id=access_point.chat_id,
            reply_markup={"inline_keyboard": [buttons]},
            parse_mode="HTML",
            priority=ACCESS_POINT_OUTBOUND_CLASS_SEND,
            ordering_key=access_point,
            on_success=lambda result, p=pending, req=request, ap=access_point, src=source: self._on_approval_prompt_sent(
                pending=p,
                send_result=result,
                req_id=req.req_id,
                access_point=ap,
                source=src,
            ),
            on_failure=lambda exc, p=pending, req=request, ap=access_point, src=source: self._on_approval_prompt_failed(
                pending=p,
                exc=exc,
                req_id=req.req_id,
                access_point=ap,
                source=src,
            ),
        )
        return pending

    def handle_pending_approval_text(
        self,
        *,
        inbound: StewardInboundText,
        pending_approval: PendingTelegramStewardApproval | None,
    ) -> bool:
        pending = pending_approval
        if pending is None:
            return False
        if inbound.access_point.chat_id != pending.access_point.chat_id:
            return False
        if pending.access_point.thread_id is not None and inbound.access_point.thread_id != pending.access_point.thread_id:
            return False
        return True

    def approval_action_from_callback(
        self,
        *,
        update: TelegramCallbackUpdate,
        pending_approval: PendingTelegramStewardApproval | None,
    ) -> StewardApprovalDecision | StewardApprovalDetailsRequest | None:
        pending = pending_approval
        if pending is None:
            return None
        if pending.prompt_state != AccessPointDeliveryState.SENT:
            return None
        if update.chat_id != pending.access_point.chat_id:
            return None
        if pending.access_point.thread_id is not None and update.thread_id != pending.access_point.thread_id:
            return None
        if isinstance(pending.prompt_message_id, int) and update.message_id != pending.prompt_message_id:
            return None
        if update.data == "approval:details":
            if isinstance(update.callback_query_id, str):
                self._enqueue_answer_callback_query(
                    callback_query_id=update.callback_query_id,
                    text="details sent",
                    priority=ACCESS_POINT_OUTBOUND_CLASS_CALLBACK_ACK,
                    ordering_key=pending.access_point,
                )
            return StewardApprovalDetailsRequest(access_point=pending.access_point)
        if update.data not in ("approval:accept", "approval:decline", "approval:always_allow"):
            return None
        decision = update.data.split(":", 1)[1]
        if decision == "always_allow" and not supports_session_approval(
            method=pending.request.method,
            params=pending.request.params,
        ):
            return None
        if isinstance(update.callback_query_id, str):
            self._enqueue_answer_callback_query(
                callback_query_id=update.callback_query_id,
                text=f"selected: {decision}",
                priority=ACCESS_POINT_OUTBOUND_CLASS_CALLBACK_ACK,
                ordering_key=pending.access_point,
            )
        return StewardApprovalDecision(
            access_point=pending.access_point,
            decision=("always_allow" if decision == "always_allow" else decision),
            via="button",
        )

    def preprocess_inbound(
        self,
        *,
        inbound: StewardInboundText,
        pending_approval: PendingTelegramStewardApproval | None,
        handle_approval_action: Callable[[StewardApprovalDecision | StewardApprovalDetailsRequest], bool],
        handle_invalid_pending_approval_input: Callable[[], bool],
    ) -> bool:
        pending = pending_approval
        if not self.handle_pending_approval_text(inbound=inbound, pending_approval=pending):
            return False
        if pending is None:
            return True
        raw = inbound.text.strip().lower()
        if raw in ("accept", "decline") or (
            raw == "always_allow"
            and supports_session_approval(
                method=pending.request.method,
                params=pending.request.params,
            )
        ):
            return handle_approval_action(
                StewardApprovalDecision(access_point=inbound.access_point, decision=raw, via="text")
            )
        return handle_invalid_pending_approval_input()

    def send_approval_details(self, pending_approval: PendingTelegramStewardApproval) -> None:
        output_runtime, _status_store = self.status_runtime_for(
            access_point=pending_approval.access_point,
            source="agent" if pending_approval.source == "agent" else "steward",
        )
        self._enqueue_send_text(
            output_runtime=output_runtime,
            text=_format_steward_approval_details(pending_approval.request),
            kind=TelegramKind.APPROVAL_DETAILS,
            chat_id=pending_approval.access_point.chat_id,
            ordering_key=pending_approval.access_point,
        )

    def send_invalid_approval_reply(self, pending_approval: PendingTelegramStewardApproval) -> None:
        output_runtime, _status_store = self.status_runtime_for(
            access_point=pending_approval.access_point,
            source="agent" if pending_approval.source == "agent" else "steward",
        )
        allow_always = supports_session_approval(
            method=pending_approval.request.method,
            params=pending_approval.request.params,
        )
        self._enqueue_send_text(
            output_runtime=output_runtime,
            text=(
                "reply with accept, decline, or always_allow"
                if allow_always
                else "reply with accept or decline"
            ),
            kind=TelegramKind.APPROVAL_INVALID,
            chat_id=pending_approval.access_point.chat_id,
            ordering_key=pending_approval.access_point,
        )

    def clear_approval_markup(self, pending_approval: PendingTelegramStewardApproval) -> None:
        prompt_message_id = pending_approval.prompt_message_id
        if not isinstance(prompt_message_id, int):
            return
        self._enqueue_edit_reply_markup(
            chat_id=int(pending_approval.access_point.chat_id),
            message_id=prompt_message_id,
            reply_markup={"inline_keyboard": []},
            priority=ACCESS_POINT_OUTBOUND_CLASS_APPROVAL_CLEANUP,
            ordering_key=pending_approval.access_point,
            on_failure=lambda exc, pending=pending_approval, mid=prompt_message_id: self._logger.event(
                "telegram_approval_markup_clear_error",
                chat_id=pending.access_point.chat_id,
                thread_id=pending.access_point.thread_id,
                message_id=mid,
                error=str(exc),
                error_type=type(exc).__name__,
            ),
        )

    def cancel_pending_approval_prompt(self, pending_approval: PendingTelegramStewardApproval) -> None:
        queue_token = pending_approval.prompt_queue_token
        if not isinstance(queue_token, int):
            return
        item = self._outbound.find(queue_token)
        if item is not None:
            self._logger.event(
                "access_point_delivery_cancelled",
                source="telegram_steward",
                **access_point_delivery_log_fields(item),
                attempts=item.attempt_count,
                cancellation_reason="approval_no_longer_pending",
            )
            self._outbound.cancel(item)
            pending_approval.prompt_queue_token = None
            pending_approval.prompt_state = AccessPointDeliveryState.CANCELLED
            self._logger.event(
                "telegram_approval_prompt_cancelled",
                chat_id=pending_approval.access_point.chat_id,
                thread_id=pending_approval.access_point.thread_id,
                id=pending_approval.request.req_id,
                route_target=pending_approval.source,
            )
            return

    def queue_text_reply(
        self,
        *,
        access_point: AccessPointKey,
        text: str,
        source: str,
        kind: TelegramKind,
        on_sent: Callable[[], None],
        on_failed: Callable[[Exception], None],
    ) -> None:
        output_runtime, _status_store = self.status_runtime_for(
            access_point=access_point,
            source="agent" if source == "agent" else "steward",
        )
        self._enqueue_send_text(
            output_runtime=output_runtime,
            text=self._decorate_reply(text=text, source=source),
            kind=kind,
            chat_id=access_point.chat_id,
            priority=ACCESS_POINT_OUTBOUND_CLASS_SEND,
            on_success=lambda _result: on_sent(),
            on_failure=on_failed,
            ordering_key=access_point,
        )

    def queue_routing_copy(
        self,
        *,
        access_point: AccessPointKey,
        text: str,
        kind: TelegramKind,
        on_sent: Callable[[], None],
        on_failed: Callable[[Exception], None],
    ) -> None:
        self.queue_text_reply(
            access_point=access_point,
            text=text,
            source="agent",
            kind=kind,
            on_sent=on_sent,
            on_failed=on_failed,
        )

    def queue_text_reply_with_result(
        self,
        *,
        access_point: AccessPointKey,
        text: str,
        source: str,
        kind: TelegramKind,
        on_sent: Callable[[AccessPointMessageRef], None],
        on_failed: Callable[[Exception], None],
    ) -> int:
        output_runtime, _status_store = self.status_runtime_for(
            access_point=access_point,
            source="agent" if source == "agent" else "steward",
        )
        return self._enqueue_send_text(
            output_runtime=output_runtime,
            text=self._decorate_reply(text=text, source=source),
            kind=kind,
            chat_id=access_point.chat_id,
            priority=ACCESS_POINT_OUTBOUND_CLASS_SEND,
            on_success=lambda result, ap=access_point: self._on_text_reply_with_result_sent(
                access_point=ap,
                result=result,
                on_sent=on_sent,
                on_failed=on_failed,
            ),
            on_failure=on_failed,
            ordering_key=access_point,
            retry_policy=AccessPointRetryPolicy.SUPERSEDABLE,
        )

    def supersede_text_reply(
        self,
        *,
        queue_token: int,
        text: str,
        source: str,
        kind: TelegramKind,
        reason: str,
    ) -> bool:
        item = self._outbound.find(queue_token)
        if item is not None:
            if (
                item.retry_policy != AccessPointRetryPolicy.SUPERSEDABLE
                or (item.attempt_count <= 0 and item.operation != "edit_text")
                or item.replace_payload is None
            ):
                return False
            decorated = self._decorate_reply(text=text, source=source)
            if item.operation == "edit_text":
                rendered = (
                    render_session_references_html(decorated)
                    if kind in {TelegramKind.COMMAND, TelegramKind.RESTORE, TelegramKind.SESSION}
                    else decorated
                )
                item.replace_payload((rendered, "HTML" if rendered != decorated else None))
            else:
                item.replace_payload(decorated)
            self._logger.event(
                "access_point_delivery_superseded",
                source="telegram_steward",
                **access_point_delivery_log_fields(item),
                supersession_reason=reason,
            )
            return True
        return False

    def queue_text_reply_edit(
        self,
        *,
        access_point: AccessPointKey,
        message_ref: AccessPointMessageRef,
        text: str,
        source: str,
        kind: TelegramKind,
        on_sent: Callable[[], None],
        on_failed: Callable[[Exception], None],
    ) -> int | None:
        if message_ref.access_point != access_point or not isinstance(message_ref.transport_id, int):
            on_failed(RuntimeError("invalid Telegram message reference for lifecycle edit"))
            return None
        decorated = self._decorate_reply(text=text, source=source)
        rendered = (
            render_session_references_html(decorated)
            if kind in {TelegramKind.COMMAND, TelegramKind.RESTORE, TelegramKind.SESSION}
            else decorated
        )
        return self._enqueue_edit_text(
            chat_id=int(access_point.chat_id),
            message_id=message_ref.transport_id,
            text=rendered,
            parse_mode="HTML" if rendered != decorated else None,
            priority=ACCESS_POINT_OUTBOUND_CLASS_LIFECYCLE_UPDATE,
            on_success=lambda _result: on_sent(),
            on_failure=on_failed,
            ordering_key=access_point,
            retry_policy=AccessPointRetryPolicy.SUPERSEDABLE,
        )

    @staticmethod
    def _on_text_reply_with_result_sent(
        *,
        access_point: AccessPointKey,
        result: _TelegramOutboundExecutionResult,
        on_sent: Callable[[AccessPointMessageRef], None],
        on_failed: Callable[[Exception], None],
    ) -> None:
        if not isinstance(result.first_message_id, int):
            on_failed(RuntimeError("Telegram lifecycle send did not return a message id"))
            return
        on_sent(
            AccessPointMessageRef(
                access_point=access_point,
                transport_id=result.first_message_id,
            )
        )

    def send_outbound_note(
        self,
        *,
        access_point: AccessPointKey,
        source: str,
        text: str,
        kind: TelegramKind | None = None,
    ) -> None:
        output_runtime, _status_store = self.status_runtime_for(
            access_point=access_point,
            source="agent" if source == "agent" else "steward",
        )
        self._enqueue_send_text(
            output_runtime=output_runtime,
            text=text,
            kind=kind or TelegramKind.APPROVAL_ACK,
            chat_id=access_point.chat_id,
            priority=ACCESS_POINT_OUTBOUND_CLASS_APPROVAL_NOTICE,
            ordering_key=access_point,
        )

    def decorate_reply(self, *, text: str, source: str) -> str:
        return self._decorate_reply(text=text, source=source)

    def inbound_text_from_update(self, update: TelegramTextUpdate) -> StewardInboundText:
        return StewardInboundText(
            access_point=AccessPointKey(type="telegram", chat_id=update.chat_id, thread_id=update.thread_id),
            text=str(update.text),
        )

    def _decorate_reply(self, *, text: str, source: str) -> str:
        if source == "agent":
            return f"🚀 {text}"
        if source == "steward":
            return f"🧑‍✈️ {text}"
        return text

    def _enqueue_status_flush(
        self,
        *,
        access_point: AccessPointKey,
        source: str,
        output_runtime: TelegramOutputRuntime,
        candidate: TelegramDueStatusCandidate,
    ) -> None:
        coalesce_key = ("status", source, access_point, candidate.status_key)
        if self._outbound.contains_coalesce_key(coalesce_key):
            return
        self._logger.event(
            "telegram_status_global_candidate_selected",
            source="telegram_steward",
            status_key=candidate.status_key,
            thread_id=candidate.thread_id,
            last_flush_ts=candidate.last_flush_ts,
        )
        self._enqueue_outbound(
            op_kind="status",
            priority=(
                ACCESS_POINT_OUTBOUND_CLASS_SEND
                if candidate.delivery_kind == "send"
                else ACCESS_POINT_OUTBOUND_CLASS_EDIT
            ),
            coalesce_key=coalesce_key,
            execute=lambda runtime=output_runtime, status_key=candidate.status_key: _TelegramOutboundExecutionResult(
                sent=bool(runtime.flush_status(status_key=status_key))
            ),
            on_success=None,
            on_failure=lambda _exc, runtime=output_runtime: runtime.discard_pending_status_updates(),
            ordering_key=access_point,
            retry_policy=AccessPointRetryPolicy.SUPERSEDABLE,
            deduplicate=True,
        )

    def _enqueue_current_turn_status_flush(
        self,
        *,
        access_point: AccessPointKey,
        source: str,
        output_runtime: TelegramOutputRuntime,
        status_store: TurnStatusStore,
    ) -> None:
        status_key = status_store.current_turn_status_key()
        if not isinstance(status_key, str) or not status_key:
            return
        coalesce_key = ("status", source, access_point, status_key)
        if self._outbound.contains_coalesce_key(coalesce_key):
            return
        self._enqueue_outbound(
            op_kind="status",
            priority=(
                ACCESS_POINT_OUTBOUND_CLASS_SEND
                if output_runtime.status_delivery_kind(status_key=status_key) == "send"
                else ACCESS_POINT_OUTBOUND_CLASS_EDIT
            ),
            coalesce_key=coalesce_key,
            execute=lambda runtime=output_runtime, key=status_key: _TelegramOutboundExecutionResult(
                sent=bool(runtime.flush_status(status_key=key))
            ),
            on_success=None,
            on_failure=lambda _exc, runtime=output_runtime: runtime.discard_pending_status_updates(),
            ordering_key=access_point,
            retry_policy=AccessPointRetryPolicy.SUPERSEDABLE,
            deduplicate=True,
        )

    def _drop_queued_status_updates(
        self,
        *,
        access_point: AccessPointKey,
        sources: set[str],
    ) -> int:
        def should_remove(item: QueuedAccessPointOutbound[_TelegramOutboundExecutionResult]) -> bool:
            key = item.coalesce_key
            return (
                str(item.operation or "") == "status"
                and item.ordering_key == access_point
                and isinstance(key, tuple)
                and len(key) >= 2
                and key[0] == "status"
                and key[1] in sources
            )

        return len(self._outbound.cancel_matching(should_remove))

    def _log_pending_status_updates_dropped(
        self,
        *,
        access_point: AccessPointKey,
        removed: int,
    ) -> None:
        self._logger.event(
            "telegram_pending_status_updates_dropped",
            source="telegram_steward",
            chat_id=access_point.chat_id,
            thread_id=access_point.thread_id,
            removed=removed,
        )

    def _enqueue_send_text(
        self,
        *,
        output_runtime: TelegramOutputRuntime,
        text: str,
        kind: TelegramKind,
        chat_id: int,
        reply_markup: dict | None = None,
        parse_mode: str | None = None,
        priority: int = ACCESS_POINT_OUTBOUND_CLASS_SEND,
        on_success: Callable[[_TelegramOutboundExecutionResult], None] | None = None,
        on_failure: Callable[[Exception], None] | None = None,
        ordering_key: AccessPointKey | None = None,
        retry_policy: AccessPointRetryPolicy = AccessPointRetryPolicy.DURABLE,
    ) -> int:
        payload = {"text": text}
        return self._enqueue_outbound(
            op_kind=f"send_text:{kind}",
            priority=priority,
            coalesce_key=None,
            execute=lambda runtime=output_runtime, body=payload, body_kind=kind, cid=chat_id, markup=reply_markup, mode=parse_mode: self._execute_send_text(
                output_runtime=runtime,
                text=str(body["text"]),
                kind=body_kind,
                chat_id=cid,
                reply_markup=markup,
                parse_mode=mode,
            ),
            on_success=on_success,
            on_failure=on_failure,
            ordering_key=ordering_key,
            retry_policy=retry_policy,
            replace_payload=lambda replacement, body=payload: body.__setitem__("text", replacement),
        )

    def _execute_send_text(
        self,
        *,
        output_runtime: TelegramOutputRuntime,
        text: str,
        kind: TelegramKind,
        chat_id: int,
        reply_markup: dict | None,
        parse_mode: str | None,
    ) -> _TelegramOutboundExecutionResult:
        result = output_runtime.send_text(
            text=text,
            kind=kind,
            chat_id=chat_id,
            reply_markup=reply_markup,
            parse_mode=parse_mode,
            raise_on_error=True,
        )
        return _TelegramOutboundExecutionResult(
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
        on_success: Callable[[_TelegramOutboundExecutionResult], None] | None = None,
        on_failure: Callable[[Exception], None] | None = None,
        ordering_key: AccessPointKey | None = None,
    ) -> int:
        return self._enqueue_outbound(
            op_kind="edit_reply_markup",
            priority=priority,
            coalesce_key=("edit_reply_markup", int(chat_id), int(message_id)),
            execute=lambda cid=chat_id, mid=message_id, markup=reply_markup: self._execute_edit_reply_markup(
                chat_id=cid,
                message_id=mid,
                reply_markup=markup,
            ),
            on_success=on_success,
            on_failure=on_failure,
            ordering_key=ordering_key,
        )

    def _enqueue_edit_text(
        self,
        *,
        chat_id: int,
        message_id: int,
        text: str,
        parse_mode: str | None = None,
        priority: int = ACCESS_POINT_OUTBOUND_CLASS_EDIT,
        on_success: Callable[[_TelegramOutboundExecutionResult], None] | None = None,
        on_failure: Callable[[Exception], None] | None = None,
        ordering_key: AccessPointKey | None = None,
        retry_policy: AccessPointRetryPolicy = AccessPointRetryPolicy.DURABLE,
    ) -> int:
        payload = {"text": text, "parse_mode": parse_mode}
        return self._enqueue_outbound(
            op_kind="edit_text",
            priority=priority,
            coalesce_key=("edit_text", int(chat_id), int(message_id)),
            execute=lambda cid=chat_id, mid=message_id, body=payload: self._execute_edit_text(
                chat_id=cid,
                message_id=mid,
                text=str(body["text"]),
                parse_mode=body["parse_mode"],
            ),
            on_success=on_success,
            on_failure=on_failure,
            ordering_key=ordering_key,
            retry_policy=retry_policy,
            replace_payload=lambda replacement, body=payload: body.update(
                text=replacement[0], parse_mode=replacement[1],
            ),
        )

    def _execute_edit_text(
        self,
        *,
        chat_id: int,
        message_id: int,
        text: str,
        parse_mode: str | None,
    ) -> _TelegramOutboundExecutionResult:
        kwargs = {"parse_mode": parse_mode} if parse_mode is not None else {}
        self._client.edit_message_text(chat_id=chat_id, message_id=message_id, text=text, **kwargs)
        return _TelegramOutboundExecutionResult(sent=True)

    def _execute_edit_reply_markup(
        self,
        *,
        chat_id: int,
        message_id: int,
        reply_markup: dict | None,
    ) -> _TelegramOutboundExecutionResult:
        self._client.edit_message_reply_markup(
            chat_id=chat_id,
            message_id=message_id,
            reply_markup=reply_markup,
        )
        return _TelegramOutboundExecutionResult(sent=True)

    def _enqueue_answer_callback_query(
        self,
        *,
        callback_query_id: str,
        text: str | None,
        priority: int = ACCESS_POINT_OUTBOUND_CLASS_CALLBACK_ACK,
        on_success: Callable[[_TelegramOutboundExecutionResult], None] | None = None,
        on_failure: Callable[[Exception], None] | None = None,
        ordering_key: AccessPointKey | None = None,
    ) -> int:
        return self._enqueue_outbound(
            op_kind="answer_callback_query",
            priority=priority,
            coalesce_key=None,
            execute=lambda cbid=callback_query_id, body=text: self._execute_answer_callback_query(
                callback_query_id=cbid,
                text=body,
            ),
            on_success=on_success,
            on_failure=on_failure,
            ordering_key=ordering_key,
            retry_policy=AccessPointRetryPolicy.EPHEMERAL,
            expires_at=clock.monotonic() + 30.0,
            ttl_sec=30.0,
        )

    def _execute_answer_callback_query(
        self,
        *,
        callback_query_id: str,
        text: str | None,
    ) -> _TelegramOutboundExecutionResult:
        self._client.answer_callback_query(callback_query_id, text=text)
        return _TelegramOutboundExecutionResult(sent=True)

    def _enqueue_outbound(
        self,
        *,
        op_kind: str,
        priority: int,
        coalesce_key: tuple[Any, ...] | None,
        execute: Callable[[], _TelegramOutboundExecutionResult],
        on_success: Callable[[_TelegramOutboundExecutionResult], None] | None,
        on_failure: Callable[[Exception], None] | None,
        ordering_key: AccessPointKey | None = None,
        retry_policy: AccessPointRetryPolicy = AccessPointRetryPolicy.DURABLE,
        expires_at: float | None = None,
        ttl_sec: float | None = None,
        replace_payload: Callable[[object], None] | None = None,
        deduplicate: bool = False,
    ) -> int:
        queue_token = self._outbound.enqueue(
            priority=priority,
            operation=op_kind,
            coalesce_key=coalesce_key,
            progress_on_success=True,
            execute=execute,
            on_success=on_success,
            on_failure=on_failure,
            ordering_key=ordering_key,
            retry_policy=retry_policy,
            expires_at=expires_at,
            ttl_sec=ttl_sec,
            replace_payload=replace_payload,
            deduplicate=deduplicate,
        )
        if queue_token == 0:
            return 0
        self._logger.event(
            "telegram_outbound_enqueued",
            source="telegram_steward",
            queue_token=queue_token,
            op_kind=op_kind,
            priority=priority,
            queue_size=self._outbound.count,
        )
        return queue_token

    def _drain_outbound_queue(self) -> bool:
        if not self._outbound:
            return False
        if not self._status_budget.can_send_any():
            return False
        now = (self._status_monotonic_now or clock.monotonic)()
        expired = expire_access_point_outbounds(
            self._outbound,
            now=now,
            logger=self._logger,
            source="telegram_steward",
        )
        if not self._outbound:
            return expired
        ready = self._outbound.ready(now=now)
        if not ready:
            return expired
        item = select_access_point_outbound(ready)
        op_kind = str(item.operation or "")
        if op_kind == "status" and not self._status_budget.can_send_status():
            eligible = [
                candidate
                for candidate in ready
                if candidate is not item
                and str(candidate.operation or "") != "status"
                and candidate.ordering_key is not None
                and candidate.ordering_key != item.ordering_key
            ]
            if not eligible:
                return False
            item = select_access_point_outbound(eligible)
            op_kind = str(item.operation or "")
        item.attempt_count += 1
        try:
            result = item.execute()
        except Exception as exc:
            failure_now = (self._status_monotonic_now or clock.monotonic)()
            decision = decide_access_point_outbound_failure(item, exc, now=failure_now)
            if decision.retry_scheduled:
                self._logger.event(
                    "telegram_outbound_retry_scheduled",
                    source="telegram_steward",
                    queue_token=item.queue_token,
                    op_kind=op_kind,
                    retry_after_sec=decision.retry_delay_sec,
                    queue_size=self._outbound.count,
                )
                self._logger.event(
                    "access_point_delivery_retry_scheduled",
                    source="telegram_steward",
                    **access_point_delivery_log_fields(item),
                    attempt=item.attempt_count,
                    error_class=decision.error_class,
                    retry_delay_sec=decision.retry_delay_sec,
                    retry_not_before=item.retry_not_before,
                )
                return True
            self._outbound.complete(item)
            if item.on_failure is not None:
                item.on_failure(exc)
            return True
        self._outbound.complete(item)
        if item.attempt_count > 1:
            self._logger.event(
                "access_point_delivery_recovered",
                source="telegram_steward",
                **access_point_delivery_log_fields(item),
                attempts=item.attempt_count,
            )
        if result.sent:
            self._status_budget.mark_status_sent()
            wake_durable_access_point_outbounds(
                self._outbound,
                now=(self._status_monotonic_now or clock.monotonic)(),
                logger=self._logger,
                source="telegram_steward",
            )
        if item.on_success is not None:
            item.on_success(result)
        return result.sent

    def _on_approval_prompt_sent(
        self,
        *,
        pending: PendingTelegramStewardApproval,
        send_result: _TelegramOutboundExecutionResult,
        req_id: str,
        access_point: AccessPointKey,
        source: str,
    ) -> None:
        pending.prompt_state = AccessPointDeliveryState.SENT
        pending.prompt_queue_token = None
        pending.prompt_message_id = send_result.first_message_id
        self._logger.event(
            "telegram_approval_prompt_sent",
            chat_id=access_point.chat_id,
            thread_id=access_point.thread_id,
            id=req_id,
            route_target=source,
            message_id=send_result.first_message_id,
        )

    def _on_approval_prompt_failed(
        self,
        *,
        pending: PendingTelegramStewardApproval,
        exc: Exception,
        req_id: str,
        access_point: AccessPointKey,
        source: str,
    ) -> None:
        pending.prompt_state = AccessPointDeliveryState.FAILED
        pending.prompt_queue_token = None
        pending.prompt_message_id = None
        self._logger.event(
            "telegram_approval_prompt_error",
            chat_id=access_point.chat_id,
            thread_id=access_point.thread_id,
            id=req_id,
            route_target=source,
            error=str(exc),
            error_type=type(exc).__name__,
        )
        if self._approval_prompt_failure_handler is not None:
            self._approval_prompt_failure_handler(access_point, exc)
