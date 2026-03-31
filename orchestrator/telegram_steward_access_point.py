"""Telegram access-point adapter helpers for steward runtime."""

from __future__ import annotations

from dataclasses import dataclass
import re
from typing import Any, Callable

from orchestrator import clock
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
from orchestrator.approval import ApprovalRequest, build_accept_settings
from orchestrator.processes import LifecycleLogger
from orchestrator.steward_inbound import (
    StewardApprovalDecision,
    StewardApprovalDetailsRequest,
    StewardInboundText,
)
from orchestrator.telegram_approval_helper import _shorten
from orchestrator.telegram_api_thread_driver import TelegramApiThreadDriver
from orchestrator.telegram_output_runtime import (
    TelegramBotStatusBudget,
    TelegramDueStatusCandidate,
    TelegramKind,
    TelegramOutputRuntime,
    TelegramQueuedStatusDelete,
)
from orchestrator.telegram_status import TelegramStatusConfig
from orchestrator.telegram_steward_helpers import AccessPointKey
from orchestrator.turn_status_store import TurnStatusStore
from orchestrator.telegram_bridge import TelegramCallbackUpdate, TelegramTextUpdate


@dataclass
class PendingTelegramStewardApproval:
    access_point: AccessPointKey
    source: str
    request: ApprovalRequest
    prompt_state: AccessPointDeliveryState
    prompt_receipt_id: int | None
    prompt_message_id: int | None


@dataclass(frozen=True)
class _TelegramOutboundExecutionResult:
    sent: bool
    first_message_id: int | None = None
    sent_chunks: int = 0


def _approval_source_tag(request: ApprovalRequest) -> str:
    source = str(getattr(request, "role", "") or "").strip().upper()
    if source:
        return source
    return "AGENT"


def _approval_source_icon(request: ApprovalRequest) -> str:
    role = str(getattr(request, "role", "") or "").strip().lower()
    if role in {"worker", "agent", "runtime"}:
        return "🚀"
    if role in {"steward"}:
        return "🧑‍✈️"
    if role in {"lead"}:
        return "🧠"
    return "🚀"


def _format_steward_approval_prompt(request: ApprovalRequest) -> str:
    params = request.params
    command = params.get("command")
    cwd = params.get("cwd")
    header = f"{_approval_source_icon(request)} approval needed"
    lines = [header]
    if isinstance(command, str) and command.strip():
        lines.append(f"command: {_shorten(command, limit=220)}")
    if isinstance(cwd, str) and cwd.strip():
        lines.append(f"cwd: {cwd}")
    return "\n".join(lines)


def _format_steward_approval_details(request: ApprovalRequest) -> str:
    params = request.params
    command = params.get("command")
    cwd = params.get("cwd")
    reason = params.get("reason")
    actions = params.get("commandActions")
    lines = [
        f"{_approval_source_icon(request)} approval details",
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
        self._outbound_queue: list[QueuedAccessPointOutbound[_TelegramOutboundExecutionResult]] = []
        self._next_outbound_receipt_id = 1
        self._next_outbound_sequence = 1
        self._queued_status_keys: set[tuple[Any, ...]] = set()
        self._receipts: dict[int, AccessPointDeliveryReceipt] = {}
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
        runtime_entry = self._agent_status_runtime_by_access_point.get(access_point)
        if runtime_entry is not None:
            runtime_output, runtime_store = runtime_entry
            for delete in runtime_output.take_clear_deletes():
                self._enqueue_status_delete(delete)
            runtime_store.clear(clear_output=False)
        if include_steward:
            steward_entry = self._steward_status_runtime_by_access_point.get(access_point)
            if steward_entry is not None:
                steward_output, steward_store = steward_entry
                for delete in steward_output.take_clear_deletes():
                    self._enqueue_status_delete(delete)
                steward_store.clear(clear_output=False)

    def drop_pending_status_updates(
        self,
        *,
        access_point: AccessPointKey,
        include_steward: bool = False,
    ) -> None:
        status_keys: set[str] = set()
        runtime_entry = self._agent_status_runtime_by_access_point.get(access_point)
        if runtime_entry is not None:
            runtime_output, _runtime_store = runtime_entry
            runtime_output.discard_pending_status_updates()
            status_keys.update(runtime_output.status_keys())
        if include_steward:
            steward_entry = self._steward_status_runtime_by_access_point.get(access_point)
            if steward_entry is not None:
                steward_output, _steward_store = steward_entry
                steward_output.discard_pending_status_updates()
                status_keys.update(steward_output.status_keys())
        if not status_keys:
            return
        stale_keys = {("status", key) for key in status_keys}
        if not stale_keys:
            return
        before = len(self._outbound_queue)
        self._outbound_queue = [
            item for item in self._outbound_queue if item.coalesce_key not in stale_keys
        ]
        for key in stale_keys:
            self._queued_status_keys.discard(key)
        removed = before - len(self._outbound_queue)
        if removed > 0:
            self._logger.event(
                "telegram_pending_status_updates_dropped",
                source="telegram_steward",
                chat_id=access_point.chat_id,
                thread_id=access_point.thread_id,
                removed=removed,
            )

    def flush_due_status_runtimes(self) -> bool:
        candidates: list[tuple[TelegramOutputRuntime, TelegramDueStatusCandidate]] = []
        for output_runtime, _status_store in self._steward_status_runtime_by_access_point.values():
            candidate = output_runtime.peek_due_status_candidate()
            if candidate is not None:
                candidates.append((output_runtime, candidate))
        for output_runtime, _status_store in self._agent_status_runtime_by_access_point.values():
            candidate = output_runtime.peek_due_status_candidate()
            if candidate is not None:
                candidates.append((output_runtime, candidate))
        if candidates:
            selected_runtime, selected_candidate = min(
                candidates,
                key=lambda pair: (
                    float("-inf") if pair[1].last_flush_ts is None else float(pair[1].last_flush_ts),
                    pair[1].status_key,
                ),
            )
            self._enqueue_status_flush(
                output_runtime=selected_runtime,
                candidate=selected_candidate,
            )
        return self._drain_outbound_queue()

    def has_pending_outbound(self) -> bool:
        return bool(self._outbound_queue)

    def split_status_after_approval(self, access_point: AccessPointKey) -> None:
        steward_entry = self._steward_status_runtime_by_access_point.get(access_point)
        if steward_entry is not None:
            steward_output, steward_store = steward_entry
            self._enqueue_current_turn_status_flush(
                output_runtime=steward_output,
                status_store=steward_store,
            )
            steward_store.split_current_turn()
        agent_entry = self._agent_status_runtime_by_access_point.get(access_point)
        if agent_entry is not None:
            agent_output, agent_store = agent_entry
            self._enqueue_current_turn_status_flush(
                output_runtime=agent_output,
                status_store=agent_store,
            )
            agent_store.split_current_turn()

    def build_help_text(self, *, state: str) -> str:
        lines = [
            f"access point state: {state}",
            "",
            "fallback commands:",
            "/help - show this help.",
            "/where - show current access point routing key.",
            "/status - show current runtime state and bound agent details.",
            "/stop - stop runtime agent for this access point (keeps binding; Steward stays active).",
            "/start - start runtime agent for existing binding (Steward stays active).",
            "/reset - clear binding and reset local runtime state for this access point.",
            "/steward <message> - send one message to steward (available when state is BOUND).",
        ]
        return "\n".join(lines)

    def build_where_text(self, *, access_point: AccessPointKey, state: str) -> str:
        lines = [
            "access point:",
            f"- type: {access_point.type}",
            f"- chat_id: {access_point.chat_id}",
            f"- thread_id: {access_point.thread_id if access_point.thread_id is not None else 'main'}",
            f"- state: {state}",
        ]
        return "\n".join(lines)

    def build_status_text(
        self,
        *,
        access_point: AccessPointKey,
        state: str,
        steward_rows: list[dict[str, str]],
        agent_rows: list[dict[str, str]],
        binding: dict[str, str] | None,
    ) -> str:
        runtime_agent_state = "not_running"
        if state == "BOUND_IDLE":
            runtime_agent_state = "stopped"
        if agent_rows:
            runtime_agent_state = agent_rows[0]["state"]
        lines = [
            f"state: {state}",
            f"access_point: {access_point.type} chat_id={access_point.chat_id} thread_id={access_point.thread_id if access_point.thread_id is not None else 'main'}",
            f"steward_node: {steward_rows[0]['state'] if steward_rows else 'not_started'}",
            f"runtime_agent: {runtime_agent_state}",
        ]
        if agent_rows:
            agent = agent_rows[0]
            model = str(agent.get("model", "")).strip()
            lines.extend(
                [
                    f"agent_id: {agent.get('agent_id', '')}",
                    f"cwd: {agent.get('cwd', '')}",
                    f"mode: {agent.get('mode', '')}",
                ]
            )
            if model:
                lines.append(f"model: {model}")
        elif binding:
            model = str(binding.get("model", "")).strip()
            lines.extend(
                [
                    f"agent_id: {binding.get('agent_id', '')}",
                    f"cwd: {binding.get('cwd', '')}",
                    f"mode: {binding.get('mode', '')}",
                ]
            )
            if model:
                lines.append(f"model: {model}")
        return "\n".join(lines)

    def register_approval(
        self,
        *,
        access_point: AccessPointKey,
        source: str,
        request: ApprovalRequest,
    ) -> PendingTelegramStewardApproval:
        allow_always = build_accept_settings(params=request.params) is not None
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
            prompt_receipt_id=None,
            prompt_message_id=None,
        )
        pending.prompt_receipt_id = self._enqueue_send_text(
            output_runtime=output_runtime,
            text=_format_steward_approval_prompt(request),
            kind=TelegramKind.APPROVAL_PROMPT,
            chat_id=access_point.chat_id,
            reply_markup={"inline_keyboard": [buttons]},
            parse_mode="HTML",
            priority=ACCESS_POINT_OUTBOUND_CLASS_SEND,
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
                )
            return StewardApprovalDetailsRequest(access_point=pending.access_point)
        if update.data not in ("approval:accept", "approval:decline", "approval:always_allow"):
            return None
        decision = update.data.split(":", 1)[1]
        if isinstance(update.callback_query_id, str):
            self._enqueue_answer_callback_query(
                callback_query_id=update.callback_query_id,
                text=f"selected: {decision}",
                priority=ACCESS_POINT_OUTBOUND_CLASS_CALLBACK_ACK,
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
        if raw in ("accept", "decline", "always_allow"):
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
        )

    def send_invalid_approval_reply(self, pending_approval: PendingTelegramStewardApproval) -> None:
        output_runtime, _status_store = self.status_runtime_for(
            access_point=pending_approval.access_point,
            source="agent" if pending_approval.source == "agent" else "steward",
        )
        self._enqueue_send_text(
            output_runtime=output_runtime,
            text="reply with accept, decline, or always_allow",
            kind=TelegramKind.APPROVAL_INVALID,
            chat_id=pending_approval.access_point.chat_id,
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
        receipt_id = pending_approval.prompt_receipt_id
        if not isinstance(receipt_id, int):
            return
        for item in list(self._outbound_queue):
            if item.receipt_id != receipt_id:
                continue
            self._finish_outbound_item(item)
            pending_approval.prompt_receipt_id = None
            pending_approval.prompt_state = AccessPointDeliveryState.CANCELLED
            receipt = self._receipts.get(receipt_id)
            if receipt is not None:
                receipt.mark_cancelled()
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
        output_runtime: TelegramOutputRuntime,
        candidate: TelegramDueStatusCandidate,
    ) -> None:
        coalesce_key = ("status", candidate.status_key)
        if coalesce_key in self._queued_status_keys:
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
            on_failure=None,
        )
        self._queued_status_keys.add(coalesce_key)

    def _enqueue_current_turn_status_flush(
        self,
        *,
        output_runtime: TelegramOutputRuntime,
        status_store: TurnStatusStore,
    ) -> None:
        status_key = status_store.current_turn_status_key()
        if not isinstance(status_key, str) or not status_key:
            return
        coalesce_key = ("status", status_key)
        if coalesce_key in self._queued_status_keys:
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
            on_failure=None,
        )
        self._queued_status_keys.add(coalesce_key)

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
    ) -> int:
        return self._enqueue_outbound(
            op_kind=f"send_text:{kind}",
            priority=priority,
            coalesce_key=None,
            execute=lambda runtime=output_runtime, body=text, body_kind=kind, cid=chat_id, markup=reply_markup, mode=parse_mode: self._execute_send_text(
                output_runtime=runtime,
                text=body,
                kind=body_kind,
                chat_id=cid,
                reply_markup=markup,
                parse_mode=mode,
            ),
            on_success=on_success,
            on_failure=on_failure,
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
        )

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
        )

    def _execute_answer_callback_query(
        self,
        *,
        callback_query_id: str,
        text: str | None,
    ) -> _TelegramOutboundExecutionResult:
        self._client.answer_callback_query(callback_query_id, text=text)
        return _TelegramOutboundExecutionResult(sent=True)

    def _enqueue_status_delete(
        self,
        delete: TelegramQueuedStatusDelete,
    ) -> None:
        self._enqueue_outbound(
            op_kind="delete_status",
            priority=ACCESS_POINT_OUTBOUND_CLASS_EDIT,
            coalesce_key=("delete_status", delete.status_key, int(delete.message_id)),
            execute=lambda task=delete: self._execute_status_delete(task),
            on_success=None,
            on_failure=lambda exc, task=delete: self._logger.event(
                "telegram_status_delete_error",
                source=task.source,
                status_key=task.status_key,
                chat_id=task.chat_id,
                message_id=task.message_id,
                thread_id=task.thread_id,
                error=str(exc),
                error_type=type(exc).__name__,
                retry_after_sec=_extract_retry_after_sec(exc),
            ),
        )

    def _execute_status_delete(
        self,
        delete: TelegramQueuedStatusDelete,
    ) -> _TelegramOutboundExecutionResult:
        self._client.delete_message(chat_id=delete.chat_id, message_id=delete.message_id)
        self._logger.event(
            "telegram_op",
            source=delete.source,
            op="delete",
            kind="status",
            status_key=delete.status_key,
            chat_id=delete.chat_id,
            message_id=delete.message_id,
            thread_id=delete.thread_id,
        )
        self._logger.event(
            "telegram_status_cleared",
            source=delete.source,
            status_key=delete.status_key,
            chat_id=delete.chat_id,
            message_id=delete.message_id,
            message_count=1,
            dropped_pending_lines=delete.dropped_pending_lines,
            thread_id=delete.thread_id,
        )
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
    ) -> int:
        receipt_id = self._next_outbound_receipt_id
        self._next_outbound_receipt_id += 1
        self._receipts[receipt_id] = AccessPointDeliveryReceipt(receipt_id=receipt_id)
        item = QueuedAccessPointOutbound(
            receipt_id=receipt_id,
            priority=int(priority),
            sequence=self._next_outbound_sequence,
            operation=op_kind,
            coalesce_key=coalesce_key,
            progress_on_success=True,
            execute=execute,
            on_success=on_success,
            on_failure=on_failure,
        )
        self._next_outbound_sequence += 1
        self._outbound_queue.append(item)
        self._outbound_queue.sort(key=access_point_outbound_sort_key)
        self._logger.event(
            "telegram_outbound_enqueued",
            source="telegram_steward",
            receipt_id=receipt_id,
            op_kind=op_kind,
            priority=priority,
            queue_size=len(self._outbound_queue),
        )
        return receipt_id

    def _drain_outbound_queue(self) -> bool:
        if not self._outbound_queue:
            return False
        item = self._outbound_queue[0]
        op_kind = str(item.operation or "")
        if op_kind == "status":
            if not self._status_budget.can_send_status():
                return False
        elif not self._status_budget.can_send_any():
            return False
        try:
            result = item.execute()
        except Exception as exc:
            retry_after_sec = _extract_retry_after_sec(exc)
            if retry_after_sec is not None and retry_after_sec > 0:
                self._logger.event(
                    "telegram_outbound_retry_scheduled",
                    source="telegram_steward",
                    receipt_id=item.receipt_id,
                    op_kind=op_kind,
                    retry_after_sec=retry_after_sec,
                    queue_size=len(self._outbound_queue),
                )
                return True
            receipt = self._receipts.get(item.receipt_id)
            if receipt is not None:
                receipt.mark_failed(exc)
            self._finish_outbound_item(item)
            if item.on_failure is not None:
                item.on_failure(exc)
            return True
        receipt = self._receipts.get(item.receipt_id)
        if receipt is not None:
            receipt.mark_sent(message_id=result.first_message_id)
        self._finish_outbound_item(item)
        if result.sent:
            self._status_budget.mark_status_sent()
        if item.on_success is not None:
            item.on_success(result)
        return result.sent

    def _finish_outbound_item(self, item: QueuedAccessPointOutbound[_TelegramOutboundExecutionResult]) -> None:
        if self._outbound_queue and self._outbound_queue[0] is item:
            self._outbound_queue.pop(0)
        else:
            try:
                self._outbound_queue.remove(item)
            except ValueError:
                pass
        if item.coalesce_key is not None:
            self._queued_status_keys.discard(item.coalesce_key)

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
        pending.prompt_receipt_id = None
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
        pending.prompt_receipt_id = None
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


def _extract_retry_after_sec(exc: Exception) -> int | None:
    match = re.search(r"retry after (\d+)", str(exc), flags=re.IGNORECASE)
    if not match:
        return None
    try:
        return int(match.group(1))
    except ValueError:
        return None
