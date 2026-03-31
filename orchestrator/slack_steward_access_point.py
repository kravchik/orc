"""Slack access-point adapter helpers for steward runtime."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Callable
from typing import Any

from orchestrator.access_point_common import (
    ACCESS_POINT_OUTBOUND_CLASS_APPROVAL_CLEANUP,
    ACCESS_POINT_OUTBOUND_CLASS_EDIT,
    ACCESS_POINT_OUTBOUND_CLASS_SEND,
    AccessPointDeliveryReceipt,
    AccessPointDeliveryState,
    QueuedAccessPointOutbound,
    access_point_outbound_sort_key,
)
from orchestrator.approval import ApprovalRequest, build_accept_settings
from orchestrator.processes import LifecycleLogger
from orchestrator.slack_api_thread_driver import SlackApiThreadDriver
from orchestrator.slack_interactivity import SlackInteractivityAction
from orchestrator.slack_output_runtime import SlackOutputRuntime
from orchestrator.steward_inbound import (
    StewardApprovalDecision,
    StewardApprovalDetailsRequest,
    StewardInboundText,
)
from orchestrator.telegram_status import TelegramStatusConfig
from orchestrator.telegram_steward_helpers import AccessPointKey
from orchestrator.turn_status_store import TurnStatusStore


@dataclass
class SlackStewardOutboundMessage:
    channel_id: str
    thread_ts: str | None
    text: str
    blocks: list[dict] | None = None


@dataclass
class PendingSlackStewardApproval:
    access_point: AccessPointKey
    source: str
    request: ApprovalRequest
    prompt_state: AccessPointDeliveryState
    prompt_receipt_id: int | None
    prompt_ts: str | None


def _build_approval_blocks(*, allow_always: bool) -> list[dict]:
    buttons = [
        {
            "type": "button",
            "text": {"type": "plain_text", "text": "Accept"},
            "action_id": "approval_accept",
            "value": "accept",
        },
        {
            "type": "button",
            "text": {"type": "plain_text", "text": "Decline"},
            "action_id": "approval_decline",
            "value": "decline",
        },
        {
            "type": "button",
            "text": {"type": "plain_text", "text": "Details"},
            "action_id": "approval_details",
            "value": "details",
        },
    ]
    if allow_always:
        buttons.append(
            {
                "type": "button",
                "text": {"type": "plain_text", "text": "Always allow"},
                "action_id": "approval_always_allow",
                "value": "always_allow",
            }
        )
    return [{"type": "actions", "elements": buttons}]


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


def _format_approval_prompt(*, request: ApprovalRequest, allow_always: bool) -> str:
    params = request.params
    command = params.get("command")
    cwd = params.get("cwd")
    lines = [f"{_approval_source_icon(request)} approval needed"]
    if isinstance(command, str) and command.strip():
        lines.append(f"command: {command}")
    if isinstance(cwd, str) and cwd.strip():
        lines.append(f"cwd: {cwd}")
    options = "accept/decline/details"
    if allow_always:
        options += "/always_allow"
    lines.append(f"reply: {options}")
    return "\n".join(lines)


def _format_approval_details(request: ApprovalRequest) -> str:
    params = request.params
    lines = [
        f"{_approval_source_icon(request)} approval details",
        f"method={request.method}",
        f"role={getattr(request, 'role', '')}",
    ]
    for key in ("command", "cwd", "reason"):
        value = params.get(key)
        if value is not None:
            lines.append(f"{key}={value}")
    return "\n".join(lines)


class SlackStewardAccessPointAdapter:
    """Slack-specific AP rendering and approval UI for steward runtime."""

    def __init__(
        self,
        *,
        client: SlackApiThreadDriver,
        logger: LifecycleLogger,
        status_config: TelegramStatusConfig,
    ) -> None:
        self._client = client
        self._logger = logger
        self._status_config = status_config
        self._steward_status_runtime_by_access_point: dict[
            AccessPointKey,
            tuple[SlackOutputRuntime, TurnStatusStore],
        ] = {}
        self._agent_status_runtime_by_access_point: dict[
            AccessPointKey,
            tuple[SlackOutputRuntime, TurnStatusStore],
        ] = {}
        self._outbound_queue: list[QueuedAccessPointOutbound[object | None]] = []
        self._next_outbound_receipt_id = 1
        self._next_outbound_sequence = 1
        self._queued_keys: set[tuple[Any, ...]] = set()
        self._receipts: dict[int, AccessPointDeliveryReceipt] = {}

    def _log_access_point_event(self, event: str, *, access_point: AccessPointKey, **fields: object) -> None:
        self._logger.event(
            event,
            access_point_type=access_point.type,
            chat_id=access_point.chat_id,
            thread_id=access_point.thread_id,
            **fields,
        )

    def inbound_text_from_message(self, message: dict[str, Any]) -> StewardInboundText | None:
        text = message.get("text")
        if not isinstance(text, str):
            return None
        channel_id = str(message.get("channel") or "").strip()
        if not channel_id:
            return None
        ts = str(message.get("ts") or "").strip() or None
        thread_ts = str(message.get("thread_ts") or "").strip() or None
        if ts is not None and thread_ts is not None and thread_ts != ts:
            self._logger.event(
                "slack_steward_thread_reply_ignored",
                channel_id=channel_id,
                thread_ts=thread_ts,
                message_ts=ts,
                text_preview=text.strip()[:200],
            )
            return None
        inbound = StewardInboundText(
            access_point=AccessPointKey(type="slack", chat_id=channel_id, thread_id=None),
            text=text,
        )
        self._log_access_point_event(
            "slack_steward_inbound_text_parsed",
            access_point=inbound.access_point,
            message_ts=ts,
            text_preview=text.strip()[:200],
        )
        return inbound

    def approval_action_from_text(
        self,
        *,
        inbound: StewardInboundText,
        pending_approval: PendingSlackStewardApproval | None,
    ) -> StewardApprovalDecision | StewardApprovalDetailsRequest | None:
        pending = pending_approval
        if pending is None or inbound.access_point != pending.access_point:
            return None
        raw = inbound.text.strip().lower()
        if raw == "details":
            self._log_access_point_event(
                "slack_steward_approval_text_recognized",
                access_point=inbound.access_point,
                decision="details",
                via="text",
            )
            return StewardApprovalDetailsRequest(access_point=inbound.access_point)
        if raw in ("accept", "decline", "always_allow"):
            self._log_access_point_event(
                "slack_steward_approval_text_recognized",
                access_point=inbound.access_point,
                decision=raw,
                via="text",
            )
            return StewardApprovalDecision(access_point=inbound.access_point, decision=raw, via="text")
        return None

    def approval_action_from_interactive(
        self,
        *,
        action: SlackInteractivityAction,
        pending_approval: PendingSlackStewardApproval | None,
    ) -> StewardApprovalDecision | StewardApprovalDetailsRequest | None:
        pending = pending_approval
        if pending is None:
            return None
        if pending.prompt_state != AccessPointDeliveryState.SENT:
            return None
        if action.channel_id != str(pending.access_point.chat_id):
            return None
        pending_thread_ts = self._thread_ts(pending.access_point)
        if pending_thread_ts and pending_thread_ts != action.thread_ts:
            return None
        if pending.prompt_ts and action.message_ts and pending.prompt_ts != action.message_ts:
            return None
        raw = action.value.strip().lower()
        if raw == "details":
            self._log_access_point_event(
                "slack_steward_approval_action_recognized",
                access_point=pending.access_point,
                decision="details",
                via="button",
                action_id=action.action_id,
            )
            return StewardApprovalDetailsRequest(access_point=pending.access_point)
        if raw in ("accept", "decline", "always_allow"):
            self._log_access_point_event(
                "slack_steward_approval_action_recognized",
                access_point=pending.access_point,
                decision=raw,
                via="button",
                action_id=action.action_id,
            )
            return StewardApprovalDecision(access_point=pending.access_point, decision=raw, via="button")
        return None

    def preprocess_inbound(
        self,
        *,
        inbound: StewardInboundText,
        pending_approval: PendingSlackStewardApproval | None,
        handle_approval_action: Any,
        handle_invalid_pending_approval_input: Any,
    ) -> bool:
        pending = pending_approval
        if pending is None or inbound.access_point != pending.access_point:
            return False
        approval_action = self.approval_action_from_text(
            inbound=inbound,
            pending_approval=pending,
        )
        if approval_action is None:
            self._log_access_point_event(
                "slack_steward_pending_approval_invalid_input",
                access_point=inbound.access_point,
                text_preview=inbound.text.strip()[:200],
            )
            return bool(handle_invalid_pending_approval_input())
        self._log_access_point_event(
            "slack_steward_pending_approval_input_handled",
            access_point=inbound.access_point,
            action_type=type(approval_action).__name__,
        )
        return bool(handle_approval_action(approval_action))

    def register_approval(
        self,
        *,
        access_point: AccessPointKey,
        source: str,
        request: ApprovalRequest,
    ) -> PendingSlackStewardApproval:
        allow_always = build_accept_settings(params=request.params) is not None
        pending = PendingSlackStewardApproval(
            access_point=access_point,
            source=source,
            request=request,
            prompt_state=AccessPointDeliveryState.PENDING,
            prompt_receipt_id=None,
            prompt_ts=None,
        )
        pending.prompt_receipt_id = self._enqueue_outbound(
            priority=ACCESS_POINT_OUTBOUND_CLASS_SEND,
            operation="post_message",
            telemetry={
                "kind": "approval_prompt",
                "channel_id": str(access_point.chat_id),
                "thread_ts": self._thread_ts(access_point),
                "source": source,
                "text_len": len(_format_approval_prompt(request=request, allow_always=allow_always)),
                "has_blocks": True,
            },
            execute=lambda ap=access_point, req=request, aa=allow_always: self._client.post_message(
                channel_id=str(ap.chat_id),
                thread_ts=self._thread_ts(ap),
                text=_format_approval_prompt(request=req, allow_always=aa),
                blocks=_build_approval_blocks(allow_always=aa),
            ),
            on_success=lambda sent, p=pending, req=request, ap=access_point, src=source: self._on_approval_prompt_sent(
                pending=p,
                sent=sent,
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

    def send_approval_details(self, pending_approval: PendingSlackStewardApproval) -> None:
        details = _format_approval_details(pending_approval.request)
        self._enqueue_outbound(
            priority=ACCESS_POINT_OUTBOUND_CLASS_SEND,
            operation="post_message",
            telemetry={
                "kind": "approval_details",
                "channel_id": str(pending_approval.access_point.chat_id),
                "thread_ts": self._thread_ts(pending_approval.access_point),
                "source": pending_approval.source,
                "text_len": len(details),
                "has_blocks": False,
            },
            execute=lambda pending=pending_approval: self._client.post_message(
                channel_id=str(pending.access_point.chat_id),
                thread_ts=self._thread_ts(pending.access_point),
                text=details,
            ),
        )

    def send_invalid_approval_reply(self, pending_approval: PendingSlackStewardApproval) -> None:
        text = "reply with accept, decline, details, or always_allow"
        self._enqueue_outbound(
            priority=ACCESS_POINT_OUTBOUND_CLASS_SEND,
            operation="post_message",
            telemetry={
                "kind": "approval_invalid_reply",
                "channel_id": str(pending_approval.access_point.chat_id),
                "thread_ts": self._thread_ts(pending_approval.access_point),
                "source": pending_approval.source,
                "text_len": len(text),
                "has_blocks": False,
            },
            execute=lambda pending=pending_approval: self._client.post_message(
                channel_id=str(pending.access_point.chat_id),
                thread_ts=self._thread_ts(pending.access_point),
                text=text,
            ),
        )

    def clear_approval_blocks(self, pending_approval: PendingSlackStewardApproval) -> None:
        if pending_approval.prompt_ts is None:
            return
        allow_always = build_accept_settings(params=pending_approval.request.params) is not None
        prompt_text = _format_approval_prompt(request=pending_approval.request, allow_always=allow_always)
        self._enqueue_outbound(
            priority=ACCESS_POINT_OUTBOUND_CLASS_APPROVAL_CLEANUP,
            operation="update_message",
            telemetry={
                "kind": "approval_cleanup",
                "channel_id": str(pending_approval.access_point.chat_id),
                "thread_ts": self._thread_ts(pending_approval.access_point),
                "message_ts": pending_approval.prompt_ts,
                "source": pending_approval.source,
                "text_len": len(prompt_text),
                "has_blocks": False,
            },
            execute=lambda pending=pending_approval, aa=allow_always: self._client.update_message(
                channel_id=str(pending.access_point.chat_id),
                ts=str(pending.prompt_ts),
                text=prompt_text,
                blocks=[],
            ),
            on_failure=lambda exc, pending=pending_approval: self._logger.event(
                "slack_approval_markup_clear_error",
                channel_id=str(pending.access_point.chat_id),
                thread_ts=self._thread_ts(pending.access_point),
                message_ts=pending.prompt_ts,
                error=str(exc),
                error_type=type(exc).__name__,
            ),
        )

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
            f"- channel_id: {access_point.chat_id}",
            f"- thread_ts: {access_point.thread_id or 'main'}",
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
            f"access_point: {access_point.type} channel_id={access_point.chat_id} thread_ts={access_point.thread_id or 'main'}",
            f"steward_node: {steward_rows[0]['state'] if steward_rows else 'not_started'}",
            f"runtime_agent: {runtime_agent_state}",
        ]
        row = agent_rows[0] if agent_rows else binding
        if row:
            lines.extend(
                [
                    f"agent_id: {row.get('agent_id', '')}",
                    f"cwd: {row.get('cwd', '')}",
                    f"mode: {row.get('mode', '')}",
                ]
            )
            model = str(row.get("model", "")).strip()
            if model:
                lines.append(f"model: {model}")
        return "\n".join(lines)

    def status_runtime_for(
        self,
        *,
        access_point: AccessPointKey,
        source: str,
    ) -> tuple[SlackOutputRuntime, TurnStatusStore]:
        mapping = (
            self._steward_status_runtime_by_access_point
            if source == "steward"
            else self._agent_status_runtime_by_access_point
        )
        existing = mapping.get(access_point)
        if existing is not None:
            return existing
        channel_id = str(access_point.chat_id)
        thread_ts = self._thread_ts(access_point)
        output_runtime = SlackOutputRuntime(
            client=self._client,
            logger=self._logger,
            source=source,
            channel_id_getter=lambda cid=channel_id: cid,
            thread_ts_getter=lambda ts=thread_ts: ts,
            status_config=self._status_config,
        )
        status_store = TurnStatusStore(output_runtime=output_runtime)
        pair = (output_runtime, status_store)
        mapping[access_point] = pair
        return pair

    def clear_status_runtimes(self, *, access_point: AccessPointKey, include_steward: bool = False) -> None:
        runtime_entry = self._agent_status_runtime_by_access_point.get(access_point)
        if runtime_entry is not None:
            runtime_output, runtime_store = runtime_entry
            runtime_store.clear(clear_output=False)
            self._enqueue_outbound(
                priority=ACCESS_POINT_OUTBOUND_CLASS_EDIT,
                operation="delete_message",
                telemetry={
                    "kind": "status_clear",
                    "channel_id": str(access_point.chat_id),
                    "thread_ts": self._thread_ts(access_point),
                    "source": "agent",
                },
                coalesce_key=("status_clear", "agent", access_point),
                execute=runtime_output.clear_status,
            )
        if include_steward:
            steward_entry = self._steward_status_runtime_by_access_point.get(access_point)
            if steward_entry is not None:
                steward_output, steward_store = steward_entry
                steward_store.clear(clear_output=False)
                self._enqueue_outbound(
                    priority=ACCESS_POINT_OUTBOUND_CLASS_EDIT,
                    operation="delete_message",
                    telemetry={
                        "kind": "status_clear",
                        "channel_id": str(access_point.chat_id),
                        "thread_ts": self._thread_ts(access_point),
                        "source": "steward",
                    },
                    coalesce_key=("status_clear", "steward", access_point),
                    execute=steward_output.clear_status,
                )

    def flush_due_status_runtimes(self) -> bool:
        progressed = False
        for access_point, (output_runtime, _status_store) in self._steward_status_runtime_by_access_point.items():
            if not output_runtime.has_due_statuses():
                continue
            self._enqueue_outbound(
                priority=(
                    ACCESS_POINT_OUTBOUND_CLASS_SEND
                    if output_runtime.status_delivery_kind() == "send"
                    else ACCESS_POINT_OUTBOUND_CLASS_EDIT
                ),
                operation="status_flush",
                telemetry={
                    "kind": "status_flush",
                    "channel_id": str(access_point.chat_id),
                    "thread_ts": self._thread_ts(access_point),
                    "source": "steward",
                    "delivery_kind": output_runtime.status_delivery_kind(),
                },
                coalesce_key=("status_due", "steward", access_point),
                execute=output_runtime.flush_due_statuses,
                progress_on_success=False,
            )
        for access_point, (output_runtime, _status_store) in self._agent_status_runtime_by_access_point.items():
            if not output_runtime.has_due_statuses():
                continue
            self._enqueue_outbound(
                priority=(
                    ACCESS_POINT_OUTBOUND_CLASS_SEND
                    if output_runtime.status_delivery_kind() == "send"
                    else ACCESS_POINT_OUTBOUND_CLASS_EDIT
                ),
                operation="status_flush",
                telemetry={
                    "kind": "status_flush",
                    "channel_id": str(access_point.chat_id),
                    "thread_ts": self._thread_ts(access_point),
                    "source": "agent",
                    "delivery_kind": output_runtime.status_delivery_kind(),
                },
                coalesce_key=("status_due", "agent", access_point),
                execute=output_runtime.flush_due_statuses,
                progress_on_success=False,
            )
        return self._drain_outbound_queue() or progressed

    def has_pending_outbound(self) -> bool:
        return bool(self._outbound_queue)

    def split_status_after_approval(self, access_point: AccessPointKey) -> None:
        steward_entry = self._steward_status_runtime_by_access_point.get(access_point)
        if steward_entry is not None:
            steward_output, steward_store = steward_entry
            status_key = steward_store.current_turn_status_key()
            if status_key is not None:
                self._enqueue_outbound(
                    priority=(
                        ACCESS_POINT_OUTBOUND_CLASS_SEND
                        if steward_output.status_delivery_kind(status_key=status_key) == "send"
                        else ACCESS_POINT_OUTBOUND_CLASS_EDIT
                    ),
                    operation="status_flush",
                    telemetry={
                        "kind": "status_split_flush",
                        "channel_id": str(access_point.chat_id),
                        "thread_ts": self._thread_ts(access_point),
                        "source": "steward",
                        "delivery_kind": steward_output.status_delivery_kind(status_key=status_key),
                        "status_key": status_key,
                    },
                    coalesce_key=("status_flush", "steward", access_point, status_key),
                    execute=lambda runtime=steward_output, key=status_key: runtime.flush_status(status_key=key),
                )
            steward_store.split_current_turn()
        agent_entry = self._agent_status_runtime_by_access_point.get(access_point)
        if agent_entry is not None:
            agent_output, agent_store = agent_entry
            status_key = agent_store.current_turn_status_key()
            if status_key is not None:
                self._enqueue_outbound(
                    priority=(
                        ACCESS_POINT_OUTBOUND_CLASS_SEND
                        if agent_output.status_delivery_kind(status_key=status_key) == "send"
                        else ACCESS_POINT_OUTBOUND_CLASS_EDIT
                    ),
                    operation="status_flush",
                    telemetry={
                        "kind": "status_split_flush",
                        "channel_id": str(access_point.chat_id),
                        "thread_ts": self._thread_ts(access_point),
                        "source": "agent",
                        "delivery_kind": agent_output.status_delivery_kind(status_key=status_key),
                        "status_key": status_key,
                    },
                    coalesce_key=("status_flush", "agent", access_point, status_key),
                    execute=lambda runtime=agent_output, key=status_key: runtime.flush_status(status_key=key),
                )
            agent_store.split_current_turn()

    def queue_text_reply(
        self,
        *,
        access_point: AccessPointKey,
        text: str,
        source: str,
        kind: Any = None,
        on_sent: Any = None,
        on_failed: Any = None,
    ) -> None:
        _ = kind
        reply_text = self.decorate_reply(text=text, source=source)
        self._enqueue_outbound(
            priority=ACCESS_POINT_OUTBOUND_CLASS_SEND,
            operation="post_message",
            telemetry={
                "kind": "reply",
                "channel_id": str(access_point.chat_id),
                "thread_ts": self._thread_ts(access_point),
                "source": source,
                "text_len": len(reply_text),
                "has_blocks": False,
            },
            execute=lambda ap=access_point, body=text, src=source: self._client.post_message(
                channel_id=str(ap.chat_id),
                thread_ts=self._thread_ts(ap),
                text=reply_text,
            ),
            on_success=(lambda _result: on_sent()) if callable(on_sent) else None,
            on_failure=on_failed if callable(on_failed) else None,
        )

    def send_outbound_note(
        self,
        *,
        access_point: AccessPointKey,
        source: str,
        text: str,
        kind: Any | None = None,
    ) -> None:
        _ = source
        _ = kind
        self._enqueue_outbound(
            priority=ACCESS_POINT_OUTBOUND_CLASS_SEND,
            operation="post_message",
            telemetry={
                "kind": "outbound_note",
                "channel_id": str(access_point.chat_id),
                "thread_ts": self._thread_ts(access_point),
                "text_len": len(text),
                "has_blocks": False,
            },
            execute=lambda ap=access_point, body=text: self._client.post_message(
                channel_id=str(ap.chat_id),
                thread_ts=self._thread_ts(ap),
                text=body,
            ),
        )

    def decorate_reply(self, *, text: str, source: str) -> str:
        if source == "agent":
            return f"🚀 {text}"
        if source == "steward":
            return f"🧑‍✈️ {text}"
        return text

    def _thread_ts(self, access_point: AccessPointKey) -> str | None:
        raw = access_point.thread_id
        if raw is None:
            return None
        text = str(raw).strip()
        return text or None

    def cancel_pending_approval_prompt(self, pending_approval: PendingSlackStewardApproval) -> None:
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
                "slack_approval_prompt_cancelled",
                channel_id=str(pending_approval.access_point.chat_id),
                thread_ts=self._thread_ts(pending_approval.access_point),
                route_target=pending_approval.source,
                req_id=pending_approval.request.req_id,
            )
            return

    def _on_approval_prompt_sent(
        self,
        *,
        pending: PendingSlackStewardApproval,
        sent: object | None,
        req_id: object,
        access_point: AccessPointKey,
        source: str,
    ) -> None:
        prompt_ts_raw = sent.get("ts") if isinstance(sent, dict) else None
        prompt_ts = str(prompt_ts_raw).strip() if isinstance(prompt_ts_raw, str) and str(prompt_ts_raw).strip() else None
        pending.prompt_receipt_id = None
        pending.prompt_ts = prompt_ts
        pending.prompt_state = (
            AccessPointDeliveryState.SENT if prompt_ts is not None else AccessPointDeliveryState.FAILED
        )
        self._logger.event(
            "slack_approval_prompt_sent",
            channel_id=str(access_point.chat_id),
            thread_ts=self._thread_ts(access_point),
            route_target=source,
            req_id=req_id,
        )

    def _on_approval_prompt_failed(
        self,
        *,
        pending: PendingSlackStewardApproval,
        exc: Exception,
        req_id: object,
        access_point: AccessPointKey,
        source: str,
    ) -> None:
        pending.prompt_receipt_id = None
        pending.prompt_state = AccessPointDeliveryState.FAILED
        self._logger.event(
            "slack_approval_prompt_delivery_failed",
            channel_id=str(access_point.chat_id),
            thread_ts=self._thread_ts(access_point),
            route_target=source,
            req_id=req_id,
            error=str(exc),
            error_type=type(exc).__name__,
        )

    def _enqueue_outbound(
        self,
        *,
        priority: int,
        operation: str | None = None,
        telemetry: dict[str, object] | None = None,
        execute: Callable[[], object | None],
        on_success: Callable[[object | None], None] | None = None,
        on_failure: Callable[[Exception], None] | None = None,
        coalesce_key: tuple[Any, ...] | None = None,
        progress_on_success: bool = True,
    ) -> int:
        if coalesce_key is not None and coalesce_key in self._queued_keys:
            return 0
        receipt_id = self._next_outbound_receipt_id
        self._next_outbound_receipt_id += 1
        sequence = self._next_outbound_sequence
        self._next_outbound_sequence += 1
        self._receipts[receipt_id] = AccessPointDeliveryReceipt(receipt_id=receipt_id)
        item = QueuedAccessPointOutbound(
            receipt_id=receipt_id,
            priority=int(priority),
            sequence=sequence,
            operation=operation,
            coalesce_key=coalesce_key,
            progress_on_success=bool(progress_on_success),
            execute=execute,
            on_success=on_success,
            on_failure=on_failure,
            telemetry=dict(telemetry) if isinstance(telemetry, dict) else None,
        )
        self._outbound_queue.append(item)
        if coalesce_key is not None:
            self._queued_keys.add(coalesce_key)
        return receipt_id

    def _drain_outbound_queue(self) -> bool:
        progressed = False
        while self._outbound_queue:
            item = min(self._outbound_queue, key=access_point_outbound_sort_key)
            self._log_outbound_attempt(item=item)
            try:
                result = item.execute()
            except Exception as exc:
                receipt = self._receipts.get(item.receipt_id)
                if receipt is not None:
                    receipt.mark_failed(exc)
                self._log_outbound_error(item=item, exc=exc)
                self._finish_outbound_item(item)
                if callable(item.on_failure):
                    item.on_failure(exc)
                progressed = True
                continue
            receipt = self._receipts.get(item.receipt_id)
            if receipt is not None:
                message_ts = None
                if isinstance(result, dict):
                    raw_ts = result.get("ts")
                    message_ts = raw_ts if isinstance(raw_ts, str) else None
                receipt.mark_sent(message_ts=message_ts)
            self._finish_outbound_item(item)
            if callable(item.on_success):
                item.on_success(result)
            if item.progress_on_success:
                progressed = True
            elif bool(result):
                progressed = True
        return progressed

    def _finish_outbound_item(self, item: QueuedAccessPointOutbound[object | None]) -> None:
        if item in self._outbound_queue:
            self._outbound_queue.remove(item)
        if item.coalesce_key is not None:
            self._queued_keys.discard(item.coalesce_key)

    def _log_outbound_error(self, *, item: QueuedAccessPointOutbound[object | None], exc: Exception) -> None:
        self._logger.event("slack_send_error", **self._outbound_log_fields(item=item), error=str(exc), error_type=type(exc).__name__)

    def _log_outbound_attempt(self, *, item: QueuedAccessPointOutbound[object | None]) -> None:
        self._logger.event("slack_delivery_attempt", **self._outbound_log_fields(item=item))

    def _outbound_log_fields(self, *, item: QueuedAccessPointOutbound[object | None]) -> dict[str, object]:
        fields: dict[str, object] = {
            "receipt_id": item.receipt_id,
            "operation": item.operation,
            "priority": item.priority,
            "sequence": item.sequence,
        }
        if isinstance(item.telemetry, dict):
            fields.update(item.telemetry)
        return fields
