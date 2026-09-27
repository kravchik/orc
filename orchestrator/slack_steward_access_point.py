"""Slack access-point adapter helpers for steward runtime."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable

from orchestrator import clock
from orchestrator.access_point_common import (
    ACCESS_POINT_OUTBOUND_CLASS_APPROVAL_CLEANUP,
    ACCESS_POINT_OUTBOUND_CLASS_APPROVAL_NOTICE,
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
from orchestrator.slack_api_thread_driver import SlackApiThreadDriver
from orchestrator.slack_interactivity import SlackInteractivityAction
from orchestrator.slack_output_runtime import SlackOutputRuntime
from orchestrator.steward_commands import StewardHelpContext, build_steward_help_text
from orchestrator.steward_inbound import (
    StewardApprovalDecision,
    StewardApprovalDetailsRequest,
    StewardInboundText,
)
from orchestrator.steward_restore_notice_rendering import (
    SlackRenderedMessage,
    render_session_references_slack_payload,
)
from orchestrator.telegram_status import TelegramStatusConfig
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
    prompt_queue_token: int | None
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


def _format_approval_prompt(*, request: ApprovalRequest, allow_always: bool) -> str:
    lines = [f"{approval_source_icon(request)} approval needed"]
    lines.extend(build_approval_prompt_detail_lines(request))
    options = "accept/decline/details"
    if allow_always:
        options += "/always_allow"
    lines.append(f"reply: {options}")
    return "\n".join(lines)


def _format_approval_details(request: ApprovalRequest) -> str:
    return "\n".join(
        build_approval_details_lines(
            request,
            header=f"{approval_source_icon(request)} approval details",
            include_role=True,
        )
    )


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
        self._outbound = AccessPointOutboundQueue[object | None]()

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
        access_point = self.access_point_from_message(message)
        if access_point is None:
            return None
        inbound = StewardInboundText(access_point=access_point, text=text)
        self._log_access_point_event(
            "slack_steward_inbound_text_parsed",
            access_point=inbound.access_point,
            message_ts=str(message.get("ts") or "").strip() or None,
            text_preview=text.strip()[:200],
        )
        return inbound

    def access_point_from_message(self, message: dict[str, Any]) -> AccessPointKey | None:
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
                text_preview=str(message.get("text") or "").strip()[:200],
            )
            return None
        return AccessPointKey(type="slack", chat_id=channel_id, thread_id=None)

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
        if raw in ("accept", "decline") or (
            raw == "always_allow"
            and supports_session_approval(
                method=pending.request.method,
                params=pending.request.params,
            )
        ):
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
        if raw in ("accept", "decline") or (
            raw == "always_allow"
            and supports_session_approval(
                method=pending.request.method,
                params=pending.request.params,
            )
        ):
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
        allow_always = supports_session_approval(
            method=request.method,
            params=request.params,
        )
        pending = PendingSlackStewardApproval(
            access_point=access_point,
            source=source,
            request=request,
            prompt_state=AccessPointDeliveryState.PENDING,
            prompt_queue_token=None,
            prompt_ts=None,
        )
        pending.prompt_queue_token = self._enqueue_outbound(
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
            ordering_key=access_point,
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
            ordering_key=pending_approval.access_point,
        )

    def send_invalid_approval_reply(self, pending_approval: PendingSlackStewardApproval) -> None:
        allow_always = supports_session_approval(
            method=pending_approval.request.method,
            params=pending_approval.request.params,
        )
        text = (
            "reply with accept, decline, details, or always_allow"
            if allow_always
            else "reply with accept, decline, or details"
        )
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
            ordering_key=pending_approval.access_point,
        )

    def clear_approval_blocks(self, pending_approval: PendingSlackStewardApproval) -> None:
        if pending_approval.prompt_ts is None:
            return
        allow_always = supports_session_approval(
            method=pending_approval.request.method,
            params=pending_approval.request.params,
        )
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
            ordering_key=pending_approval.access_point,
        )

    def clear_approval_markup(self, pending_approval: PendingSlackStewardApproval) -> None:
        self.clear_approval_blocks(pending_approval)

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
            status_eager_flush_when_due=False,
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
            self._logger.event(
                "slack_pending_status_updates_dropped",
                channel_id=str(access_point.chat_id),
                thread_ts=self._thread_ts(access_point),
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
            self._logger.event(
                "slack_pending_status_updates_dropped",
                channel_id=str(access_point.chat_id),
                thread_ts=self._thread_ts(access_point),
                removed=removed,
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
                on_failure=lambda _exc, runtime=output_runtime: runtime.discard_pending_status_updates(),
                progress_on_success=False,
                ordering_key=access_point,
                retry_policy=AccessPointRetryPolicy.SUPERSEDABLE,
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
                on_failure=lambda _exc, runtime=output_runtime: runtime.discard_pending_status_updates(),
                progress_on_success=False,
                ordering_key=access_point,
                retry_policy=AccessPointRetryPolicy.SUPERSEDABLE,
            )
        return self._drain_outbound_queue() or progressed

    def has_pending_outbound(self) -> bool:
        return bool(self._outbound)

    def log_delivery_handles_closed(self) -> None:
        log_abandoned_access_point_outbounds(
            self._outbound,
            logger=self._logger,
            source="slack_steward",
            access_point_type="slack",
        )
        self._logger.event(
            "slack_delivery_handles_closed",
            source="slack_steward",
            live_delivery_handles=self._outbound.count,
            peak_live_delivery_handles=self._outbound.peak_count,
        )

    def split_status(self, access_point: AccessPointKey) -> None:
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
                    on_failure=lambda _exc, runtime=steward_output: runtime.discard_pending_status_updates(),
                    ordering_key=access_point,
                    retry_policy=AccessPointRetryPolicy.SUPERSEDABLE,
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
                    on_failure=lambda _exc, runtime=agent_output: runtime.discard_pending_status_updates(),
                    ordering_key=access_point,
                    retry_policy=AccessPointRetryPolicy.SUPERSEDABLE,
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
        reply = self._render_reply(text=text, source=source, kind=kind)
        self._enqueue_outbound(
            priority=ACCESS_POINT_OUTBOUND_CLASS_SEND,
            operation="post_message",
            telemetry={
                "kind": "reply",
                "channel_id": str(access_point.chat_id),
                "thread_ts": self._thread_ts(access_point),
                "source": source,
                "text_len": len(reply.text),
                "has_blocks": reply.blocks is not None,
            },
            execute=lambda ap=access_point: self._client.post_message(
                channel_id=str(ap.chat_id),
                thread_ts=self._thread_ts(ap),
                text=reply.text,
                blocks=reply.blocks,
                mrkdwn=False if reply.plain_fallback else None,
            ),
            on_success=(lambda _result: on_sent()) if callable(on_sent) else None,
            on_failure=on_failed if callable(on_failed) else None,
            ordering_key=access_point,
        )

    def queue_routing_copy(
        self,
        *,
        access_point: AccessPointKey,
        text: str,
        kind: Any = None,
        on_sent: Any = None,
        on_failed: Any = None,
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
        kind: Any = None,
        on_sent: Any = None,
        on_failed: Any = None,
    ) -> int:
        reply = self._render_reply(text=text, source=source, kind=kind)
        payload = {"reply": reply}
        return self._enqueue_outbound(
            priority=ACCESS_POINT_OUTBOUND_CLASS_SEND,
            operation="post_message",
            telemetry={
                "kind": "reply",
                "channel_id": str(access_point.chat_id),
                "thread_ts": self._thread_ts(access_point),
                "source": source,
                "text_len": len(reply.text),
                "has_blocks": reply.blocks is not None,
            },
            execute=lambda ap=access_point, body=payload: self._client.post_message(
                channel_id=str(ap.chat_id),
                thread_ts=self._thread_ts(ap),
                text=body["reply"].text,
                blocks=body["reply"].blocks,
                mrkdwn=False if body["reply"].plain_fallback else None,
            ),
            on_success=(
                lambda result, ap=access_point: self._on_text_reply_with_result_sent(
                    access_point=ap,
                    result=result,
                    on_sent=on_sent,
                    on_failed=on_failed,
                )
                if callable(on_sent)
                else None
            ),
            on_failure=on_failed if callable(on_failed) else None,
            ordering_key=access_point,
            retry_policy=AccessPointRetryPolicy.SUPERSEDABLE,
            replace_payload=lambda replacement, body=payload: body.__setitem__("reply", replacement),
        )

    def supersede_text_reply(
        self,
        *,
        queue_token: int,
        text: str,
        source: str,
        kind: Any = None,
        reason: str,
    ) -> bool:
        reply = self._render_reply(text=text, source=source, kind=kind)
        item = self._outbound.find(queue_token)
        if item is not None:
            if (
                item.retry_policy != AccessPointRetryPolicy.SUPERSEDABLE
                or (item.attempt_count <= 0 and item.operation != "update_message")
                or item.replace_payload is None
            ):
                return False
            item.replace_payload(reply)
            self._logger.event(
                "access_point_delivery_superseded",
                source="slack_steward",
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
        kind: Any = None,
        on_sent: Any = None,
        on_failed: Any = None,
    ) -> int | None:
        ts = message_ref.transport_id
        if message_ref.access_point != access_point or not isinstance(ts, str) or not ts.strip():
            if callable(on_failed):
                on_failed(RuntimeError("invalid Slack message reference for lifecycle edit"))
            return None
        ts = ts.strip()
        reply = self._render_reply(text=text, source=source, kind=kind)
        payload = {"reply": reply}
        return self._enqueue_outbound(
            priority=ACCESS_POINT_OUTBOUND_CLASS_LIFECYCLE_UPDATE,
            operation="update_message",
            telemetry={
                "kind": "reply_edit",
                "channel_id": str(access_point.chat_id),
                "thread_ts": self._thread_ts(access_point),
                "message_ts": ts,
                "source": source,
                "text_len": len(reply.text),
                "has_blocks": reply.blocks is not None,
            },
            coalesce_key=("edit_reply", access_point, ts),
            execute=lambda ap=access_point, body=payload, message_ts=ts: self._client.update_message(
                channel_id=str(ap.chat_id),
                ts=message_ts,
                text=body["reply"].text,
                blocks=body["reply"].blocks,
                parse="full" if body["reply"].plain_fallback else None,
            ),
            on_success=(lambda _result: on_sent()) if callable(on_sent) else None,
            on_failure=on_failed if callable(on_failed) else None,
            ordering_key=access_point,
            retry_policy=AccessPointRetryPolicy.SUPERSEDABLE,
            replace_payload=lambda replacement, body=payload: body.__setitem__("reply", replacement),
        )

    @staticmethod
    def _on_text_reply_with_result_sent(
        *,
        access_point: AccessPointKey,
        result: object | None,
        on_sent: Callable[[AccessPointMessageRef], None],
        on_failed: Callable[[Exception], None],
    ) -> None:
        raw_ts = result.get("ts") if isinstance(result, dict) else None
        if not isinstance(raw_ts, str) or not raw_ts.strip():
            on_failed(RuntimeError("Slack lifecycle send did not return a message ts"))
            return
        on_sent(
            AccessPointMessageRef(
                access_point=access_point,
                transport_id=raw_ts.strip(),
            )
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
        rendered = self._render_session_references(text=text, kind=kind)
        self._enqueue_outbound(
            priority=ACCESS_POINT_OUTBOUND_CLASS_APPROVAL_NOTICE,
            operation="post_message",
            telemetry={
                "kind": "outbound_note",
                "channel_id": str(access_point.chat_id),
                "thread_ts": self._thread_ts(access_point),
                "text_len": len(rendered.text),
                "has_blocks": rendered.blocks is not None,
            },
            execute=lambda ap=access_point, body=rendered: self._client.post_message(
                channel_id=str(ap.chat_id),
                thread_ts=self._thread_ts(ap),
                text=body.text,
                blocks=body.blocks,
                mrkdwn=False if body.plain_fallback else None,
            ),
            ordering_key=access_point,
        )

    def decorate_reply(self, *, text: str, source: str) -> str:
        if source == "agent":
            return f"🚀 {text}"
        if source == "steward":
            return f"🧑‍✈️ {text}"
        return text

    def _render_reply(self, *, text: str, source: str, kind: Any) -> SlackRenderedMessage:
        return self._render_session_references(
            text=self.decorate_reply(text=text, source=source),
            kind=kind,
        )

    @staticmethod
    def _render_session_references(*, text: str, kind: Any) -> SlackRenderedMessage:
        if kind in {"command", "restore", "session"}:
            return render_session_references_slack_payload(text)
        return SlackRenderedMessage(text=text)

    def _thread_ts(self, access_point: AccessPointKey) -> str | None:
        raw = access_point.thread_id
        if raw is None:
            return None
        text = str(raw).strip()
        return text or None

    def cancel_pending_approval_prompt(self, pending_approval: PendingSlackStewardApproval) -> None:
        queue_token = pending_approval.prompt_queue_token
        if not isinstance(queue_token, int):
            return
        item = self._outbound.find(queue_token)
        if item is not None:
            self._logger.event(
                "access_point_delivery_cancelled",
                source="slack_steward",
                **access_point_delivery_log_fields(item),
                attempts=item.attempt_count,
                cancellation_reason="approval_no_longer_pending",
            )
            self._outbound.cancel(item)
            pending_approval.prompt_queue_token = None
            pending_approval.prompt_state = AccessPointDeliveryState.CANCELLED
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
        pending.prompt_queue_token = None
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
        pending.prompt_queue_token = None
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
        ordering_key: AccessPointKey | None = None,
        retry_policy: AccessPointRetryPolicy = AccessPointRetryPolicy.DURABLE,
        expires_at: float | None = None,
        ttl_sec: float | None = None,
        replace_payload: Callable[[object], None] | None = None,
    ) -> int:
        return self._outbound.enqueue(
            priority=priority,
            operation=operation,
            coalesce_key=coalesce_key,
            progress_on_success=progress_on_success,
            execute=execute,
            on_success=on_success,
            on_failure=on_failure,
            telemetry=dict(telemetry) if isinstance(telemetry, dict) else None,
            ordering_key=ordering_key,
            retry_policy=retry_policy,
            expires_at=expires_at,
            ttl_sec=ttl_sec,
            replace_payload=replace_payload,
            deduplicate=coalesce_key is not None,
        )

    def _drop_queued_status_updates(
        self,
        *,
        access_point: AccessPointKey,
        sources: set[str],
    ) -> int:
        def should_remove(item: QueuedAccessPointOutbound[object | None]) -> bool:
            telemetry = item.telemetry or {}
            return (
                str(item.operation or "") == "status_flush"
                and item.ordering_key == access_point
                and str(telemetry.get("source") or "") in sources
            )

        return len(self._outbound.cancel_matching(should_remove))

    def _drain_outbound_queue(self) -> bool:
        progressed = False
        while self._outbound:
            now = clock.monotonic()
            progressed = expire_access_point_outbounds(
                self._outbound,
                now=now,
                logger=self._logger,
                source="slack_steward",
            ) or progressed
            if not self._outbound:
                break
            item = self._outbound.next_ready(now=now)
            if item is None:
                break
            item.attempt_count += 1
            self._log_outbound_attempt(item=item)
            try:
                result = item.execute()
            except Exception as exc:
                decision = decide_access_point_outbound_failure(
                    item,
                    exc,
                    now=clock.monotonic(),
                )
                if decision.retry_scheduled:
                    self._logger.event(
                        "access_point_delivery_retry_scheduled",
                        source="slack_steward",
                        **access_point_delivery_log_fields(item),
                        attempt=item.attempt_count,
                        error_class=decision.error_class,
                        retry_delay_sec=decision.retry_delay_sec,
                        retry_not_before=item.retry_not_before,
                    )
                    progressed = True
                    continue
                self._log_outbound_error(item=item, exc=exc)
                self._outbound.complete(item)
                if callable(item.on_failure):
                    item.on_failure(exc)
                progressed = True
                continue
            self._outbound.complete(item)
            if item.attempt_count > 1:
                self._logger.event(
                    "access_point_delivery_recovered",
                    source="slack_steward",
                    **access_point_delivery_log_fields(item),
                    attempts=item.attempt_count,
                )
            if callable(item.on_success):
                item.on_success(result)
            if item.progress_on_success or bool(result):
                wake_durable_access_point_outbounds(
                    self._outbound,
                    now=clock.monotonic(),
                    logger=self._logger,
                    source="slack_steward",
                )
            if item.progress_on_success:
                progressed = True
            elif bool(result):
                progressed = True
        return progressed

    def _log_outbound_error(self, *, item: QueuedAccessPointOutbound[object | None], exc: Exception) -> None:
        self._logger.event("slack_send_error", **self._outbound_log_fields(item=item), error=str(exc), error_type=type(exc).__name__)

    def _log_outbound_attempt(self, *, item: QueuedAccessPointOutbound[object | None]) -> None:
        self._logger.event(
            "slack_delivery_attempt",
            **self._outbound_log_fields(item=item),
            attempt=item.attempt_count,
        )

    def _outbound_log_fields(self, *, item: QueuedAccessPointOutbound[object | None]) -> dict[str, object]:
        fields: dict[str, object] = {
            "queue_token": item.queue_token,
            "operation": item.operation,
            "priority": item.priority,
            "sequence": item.sequence,
        }
        if isinstance(item.telemetry, dict):
            fields.update(item.telemetry)
        return fields
