from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Protocol

from orchestrator.approval import ApprovalRequest
from orchestrator.codex_sessions import list_codex_cli_sessions
from orchestrator.processes import LifecycleLogger
from orchestrator.routing_profile_control import InMemoryRoutingProfileControl
from orchestrator.steward_actions import (
    build_action_result_prompt,
    execute_steward_actions,
    parse_steward_response,
)
from orchestrator.steward_inbound import (
    StewardApprovalDecision,
    StewardApprovalDetailsRequest,
    StewardInboundText,
)
from orchestrator.steward_runtime_support import (
    AgentDeliveryLane,
    PendingStewardOperation,
    StewardDeliveryLane,
    StewardDriverEvent,
)
from orchestrator.telegram_agent import (
    _TelegramAgentResult,
    _TelegramApprovalPromptEvent,
    _TelegramOutboundNote,
    _TelegramStatusEvent,
)
from orchestrator.telegram_bridge import extract_telegram_http_code
from orchestrator.telegram_steward_helpers import (
    AccessPointKey,
    _PersistedAccessPointState,
    access_point_sort_key,
)


class StewardAccessPointAdapter(Protocol):
    def build_help_text(self, *, state: str) -> str: ...
    def build_where_text(self, *, access_point: AccessPointKey, state: str) -> str: ...
    def build_status_text(
        self,
        *,
        access_point: AccessPointKey,
        state: str,
        steward_rows: list[dict[str, str]],
        agent_rows: list[dict[str, str]],
        binding: dict[str, str] | None,
    ) -> str: ...
    def register_approval(self, *, access_point: AccessPointKey, source: str, request: ApprovalRequest) -> Any: ...
    def preprocess_inbound(
        self,
        *,
        inbound: StewardInboundText,
        pending_approval: Any | None,
        handle_approval_action: Callable[[StewardApprovalDecision | StewardApprovalDetailsRequest], bool],
        handle_invalid_pending_approval_input: Callable[[], bool],
    ) -> bool: ...
    def send_approval_details(self, pending_approval: Any) -> None: ...
    def send_invalid_approval_reply(self, pending_approval: Any) -> None: ...
    def cancel_pending_approval_prompt(self, pending_approval: Any) -> None: ...
    def drop_pending_status_updates(
        self,
        *,
        access_point: AccessPointKey,
        include_steward: bool = False,
    ) -> None: ...
    def queue_text_reply(
        self,
        *,
        access_point: AccessPointKey,
        text: str,
        source: str,
        kind: Any,
        on_sent: Callable[[], None],
        on_failed: Callable[[Exception], None],
    ) -> None: ...
    def send_outbound_note(
        self,
        *,
        access_point: AccessPointKey,
        source: str,
        text: str,
        kind: Any | None = None,
    ) -> None: ...
    def decorate_reply(self, *, text: str, source: str) -> str: ...
    def status_runtime_for(self, *, access_point: AccessPointKey, source: str) -> tuple[Any, Any]: ...


@dataclass(frozen=True)
class StewardCoreKinds:
    reply: Any
    command: Any
    warning: Any
    restore: Any
    steward_status_source: str
    agent_status_source: str


@dataclass(frozen=True)
class StewardCoreHooks:
    approval_human_response_event: str
    response_sent_event: str
    turn_error_event: str
    access_point_fields: Callable[[AccessPointKey], dict[str, Any]]
    write_output_line: Callable[[AccessPointKey, str], str]
    on_pre_approval_decision: Callable[[Any], None] | None = None
    on_fallback_command: Callable[[AccessPointKey, str, bool], None] | None = None
    on_bound_route: Callable[[AccessPointKey, str], None] | None = None


class StewardCore:
    def __init__(
        self,
        *,
        logger: LifecycleLogger,
        writer: Callable[[str], None],
        runtime: StewardDeliveryLane,
        agent_runtime: AgentDeliveryLane,
        access_point_adapter: StewardAccessPointAdapter,
        routing_profile_control: InMemoryRoutingProfileControl,
        sessions_root: str | Path | None,
        persisted_state_by_access_point: dict[AccessPointKey, _PersistedAccessPointState],
        pending_restore_greeting: set[AccessPointKey],
        persist_registry: Callable[[str, AccessPointKey | None], None],
        drop_persisted_state: Callable[[AccessPointKey], None],
        clear_status_runtimes: Callable[[AccessPointKey, bool], None],
        split_status_after_approval: Callable[[AccessPointKey, str], None],
        kinds: StewardCoreKinds,
        hooks: StewardCoreHooks,
    ) -> None:
        self._logger = logger
        self._writer = writer
        self._runtime = runtime
        self._agent_runtime = agent_runtime
        self._access_point_adapter = access_point_adapter
        self._routing_profile_control = routing_profile_control
        self._sessions_root = sessions_root
        self._persisted_state_by_access_point = persisted_state_by_access_point
        self._pending_restore_greeting = pending_restore_greeting
        self._persist_registry = persist_registry
        self._drop_persisted_state = drop_persisted_state
        self._clear_status_runtimes = clear_status_runtimes
        self._split_status_after_approval = split_status_after_approval
        self._kinds = kinds
        self._hooks = hooks
        self._pending_text_updates: list[StewardInboundText] = []
        self._active_operations: dict[AccessPointKey, PendingStewardOperation] = {}
        self._pending_approvals: dict[AccessPointKey, Any] = {}
        self._pending_startup_notice_context: dict[AccessPointKey, str] = {}

    def _queue_local_reply(
        self,
        *,
        access_point: AccessPointKey,
        text: str,
        source: str,
        kind: Any,
    ) -> None:
        self._access_point_adapter.queue_text_reply(
            access_point=access_point,
            text=text,
            source=source,
            kind=kind,
            on_sent=lambda: None,
            on_failed=lambda exc, ap=access_point: self._logger.event(
                "steward_local_reply_delivery_failed",
                **self._hooks.access_point_fields(ap),
                source=source,
                kind=str(kind),
                error=str(exc),
                error_type=type(exc).__name__,
                http_code=extract_telegram_http_code(exc),
            ),
        )

    @property
    def pending_approval(self) -> Any | None:
        if len(self._pending_approvals) == 1:
            return next(iter(self._pending_approvals.values()))
        return None

    def pending_approval_for(self, access_point: AccessPointKey) -> Any | None:
        return self._pending_approvals.get(access_point)

    def iter_pending_approvals(self) -> list[Any]:
        return list(self._pending_approvals.values())

    def active_operation_for(self, access_point: AccessPointKey) -> PendingStewardOperation | None:
        return self._active_operations.get(access_point)

    @property
    def pending_queue_size(self) -> int:
        return len(self._pending_text_updates)

    def is_idle(self) -> bool:
        return not self._active_operations and not self._pending_text_updates and not self._pending_approvals

    def enqueue_inbound(self, inbound: StewardInboundText) -> None:
        self._pending_text_updates.append(inbound)

    def build_startup_restore_notice(self, access_point: AccessPointKey) -> str | None:
        restored = self._persisted_state_by_access_point.get(access_point)
        if restored is None:
            return None
        agent = restored.agent if isinstance(restored.agent, dict) else {}
        thread_id = str(agent.get("thread_id") or "")
        cwd = str(restored.project_cwd or agent.get("cwd") or "").strip()
        if not cwd:
            return None
        sessions = list_codex_cli_sessions(
            project_cwd=cwd,
            sessions_root=self._sessions_root,
            limit=20,
        )
        current = next((item for item in sessions if item.session_id == thread_id), None) if thread_id else None
        others = [item for item in sessions if item.session_id != thread_id][:5]
        lines = [
            "Context restored after restart.",
            f"Folder: {cwd}",
            f"Current session: {_format_restore_session_label(current, fallback_session_id=thread_id)}",
            "Other recent sessions in this folder:",
        ]
        if others:
            lines.extend(f"- {_format_restore_session_label(item)}" for item in others)
        else:
            lines.append("- none")
        lines.append(
            "Next: tell me what to do — continue the latest session, pick another one, "
            "switch folders, create a new session, make a checkout, and so on."
        )
        return "\n".join(lines)

    def emit_startup_restore_notices(self) -> None:
        pending = sorted(self._pending_restore_greeting, key=access_point_sort_key)
        self._pending_restore_greeting.clear()
        for access_point in pending:
            notice = self.build_startup_restore_notice(access_point)
            if not notice:
                continue
            self._logger.event(
                "access_point_restore_notice_enqueued",
                **self._hooks.access_point_fields(access_point),
                restored_runtime_state=self._agent_runtime.runtime_state(access_point),
            )
            self._access_point_adapter.send_outbound_note(
                access_point=access_point,
                source="steward",
                text=notice,
                kind=self._kinds.restore,
            )
            self._pending_startup_notice_context[access_point] = notice

    def _consume_startup_notice_context(self, access_point: AccessPointKey) -> str | None:
        return self._pending_startup_notice_context.pop(access_point, None)

    def handle_approval_action(self, action: StewardApprovalDecision | StewardApprovalDetailsRequest) -> bool:
        if isinstance(action, StewardApprovalDetailsRequest):
            pending = self._pending_approvals.get(action.access_point)
            if pending is not None:
                self._logger.event(
                    "steward_approval_details_requested",
                    **self._hooks.access_point_fields(pending.access_point),
                    route_target=pending.source,
                )
                self._access_point_adapter.send_approval_details(pending)
            return True
        if not isinstance(action, StewardApprovalDecision):
            raise AssertionError(f"unsupported steward approval action: {action!r}")
        pending = self._pending_approvals.get(action.access_point)
        if pending is None:
            return True
        if self._hooks.on_pre_approval_decision is not None:
            self._hooks.on_pre_approval_decision(pending)
        self._split_status_after_approval(pending.access_point, action.decision)
        fields = self._hooks.access_point_fields(pending.access_point)
        self._logger.event(
            self._hooks.approval_human_response_event,
            **fields,
            id=pending.request.req_id,
            response=action.decision,
            via=action.via,
            route_target=pending.source,
        )
        if hasattr(self._access_point_adapter, "cancel_pending_approval_prompt"):
            self._access_point_adapter.cancel_pending_approval_prompt(pending)
        if pending.source == "agent":
            self._agent_runtime.submit_approval_decision(pending.access_point, action.decision)
        else:
            self._runtime.submit_approval_decision(pending.access_point, action.decision)
        self._pending_approvals.pop(action.access_point, None)
        return True

    def handle_invalid_pending_approval_input(self, access_point: AccessPointKey) -> bool:
        pending = self._pending_approvals.get(access_point)
        if pending is None:
            return False
        self._logger.event(
            "steward_invalid_approval_input",
            **self._hooks.access_point_fields(pending.access_point),
            route_target=pending.source,
        )
        self._access_point_adapter.send_invalid_approval_reply(pending)
        return True

    def drain_pending_inputs(
        self,
        *,
        preprocess_inbound: Callable[[StewardInboundText], bool],
    ) -> bool:
        progressed = False
        idx = 0
        while idx < len(self._pending_text_updates):
            inbound = self._pending_text_updates[idx]
            access_point = inbound.access_point
            pending = self._pending_approvals.get(access_point)
            active = self._active_operations.get(access_point)
            if active is not None and pending is None:
                idx += 1
                continue
            self._pending_text_updates.pop(idx)
            if preprocess_inbound(inbound):
                progressed = True
                continue
            pending = self._pending_approvals.get(access_point)
            active = self._active_operations.get(access_point)
            if pending is not None and active is not None:
                self._pending_text_updates.insert(idx, inbound)
                idx += 1
                continue
            self.handle_text_update(inbound)
            progressed = True
        return progressed

    def build_status_text(self, *, access_point: AccessPointKey, state: str) -> str:
        steward_rows = self._runtime.show_running(access_point)
        agent_rows = self._agent_runtime.show_running(access_point)
        binding = self._agent_runtime.get_binding_info(access_point)
        return self._access_point_adapter.build_status_text(
            access_point=access_point,
            state=state,
            steward_rows=steward_rows,
            agent_rows=agent_rows,
            binding=binding,
        )

    def register_approval(self, event: StewardDriverEvent, request: ApprovalRequest) -> None:
        self._pending_approvals[event.access_point] = self._access_point_adapter.register_approval(
            access_point=event.access_point,
            source=event.source,
            request=request,
        )

    def handle_approval_prompt_delivery_failed(self, access_point: AccessPointKey, exc: Exception) -> None:
        pending = self._pending_approvals.get(access_point)
        if pending is None:
            return
        self._logger.event(
            "steward_approval_prompt_delivery_failed",
            **self._hooks.access_point_fields(access_point),
            route_target=pending.source,
            error=str(exc),
            error_type=type(exc).__name__,
            fallback_decision="decline",
        )
        self._pending_approvals.pop(access_point, None)
        if pending.source == "agent":
            self._agent_runtime.submit_approval_decision(access_point, "decline")
        else:
            self._runtime.submit_approval_decision(access_point, "decline")

    def process_steward_result(self, access_point: AccessPointKey, result: _TelegramAgentResult) -> None:
        operation = self._active_operations.get(access_point)
        if operation is None or operation.source != "steward":
            return
        initial_reply = result.reply
        plain_reply, actions = parse_steward_response(initial_reply)
        reply = plain_reply if plain_reply else initial_reply
        if not actions or operation.phase == "followup":
            final_reply = reply or operation.fallback_reply
            operation.phase = "waiting_reply_delivery"
            self._access_point_adapter.queue_text_reply(
                access_point=access_point,
                text=final_reply,
                source="steward",
                kind=operation.output_kind,
                on_sent=lambda ap=access_point, body=final_reply: self._complete_reply_delivery(
                    access_point=ap,
                    source="steward",
                    text=body,
                ),
                on_failed=lambda exc, ap=access_point: self._fail_reply_delivery(
                    access_point=ap,
                    exc=exc,
                ),
            )
            return
        action_types = [str(item.get("type") or "").strip() for item in actions]
        self._logger.event(
            "steward_actions_detected",
            access_point_type=access_point.type,
            chat_id=access_point.chat_id,
            thread_id=access_point.thread_id,
            action_count=len(actions),
            action_types=action_types,
        )
        access_point_key = (
            f"{access_point.type}:{access_point.chat_id}:"
            f"{access_point.thread_id if access_point.thread_id is not None else 'main'}"
        )
        action_results = execute_steward_actions(
            actions,
            sessions_root=self._sessions_root,
            show_running_provider=lambda: self._runtime.show_running(access_point) + self._agent_runtime.show_running(access_point),
            show_running_access_point={
                "type": access_point.type,
                "chat_id": access_point.chat_id,
                "thread_id": access_point.thread_id,
            },
            start_agent_provider=lambda spec: self._agent_runtime.start_agent(access_point, spec),
            start_agent_access_point={
                "type": access_point.type,
                "chat_id": access_point.chat_id,
                "thread_id": access_point.thread_id,
            },
            list_routing_profiles_provider=self._routing_profile_control.list_profiles,
            get_routing_profile_provider=self._routing_profile_control.get_profile,
            set_routing_profile_provider=lambda spec: self._routing_profile_control.set_profile(
                session_id=str(spec.get("session_id") or ""),
                profile=str(spec.get("profile") or ""),
                roles=dict(spec.get("roles") or {}),
            ),
            attach_routing_profile_provider=lambda spec: self._routing_profile_control.attach(
                access_point_key=access_point_key,
                session_id=str(spec.get("session_id") or ""),
                source_alias=str(spec.get("source_alias") or "@HUMAN"),
            ),
            detach_routing_profile_provider=lambda _spec: self._routing_profile_control.detach(
                access_point_key=access_point_key,
            ),
        )
        self._logger.event(
            "steward_actions_executed",
            access_point_type=access_point.type,
            chat_id=access_point.chat_id,
            thread_id=access_point.thread_id,
            action_results=action_results,
        )
        for item in action_results:
            action_type = str(item.get("type") or "").strip().upper()
            if not bool(item.get("ok")):
                continue
            if action_type == "ROUTING_PROFILE_SET":
                roles = item.get("roles")
                roles_map = dict(roles) if isinstance(roles, dict) else {}
                self._logger.event(
                    "profile_activated",
                    session_id=str(item.get("session_id") or ""),
                    profile=str(item.get("profile") or ""),
                    alias_count=len(roles_map),
                    aliases=sorted(str(alias) for alias in roles_map.keys()),
                )
            elif action_type == "ROUTING_PROFILE_ATTACH":
                self._logger.event(
                    "access_point_attached",
                    access_point_key=str(item.get("access_point_key") or access_point_key),
                    session_id=str(item.get("session_id") or ""),
                    source_alias=str(item.get("source_alias") or ""),
                )
            elif action_type == "ROUTING_PROFILE_DETACH":
                self._logger.event(
                    "access_point_detached",
                    access_point_key=str(item.get("access_point_key") or access_point_key),
                    session_id=str(item.get("session_id") or ""),
                    source_alias=str(item.get("source_alias") or ""),
                )
        if any(
            str(item.get("type") or "").strip().upper() in {"START_AGENT", "RESUME_AGENT"} and bool(item.get("ok"))
            for item in action_results
        ):
            self._clear_status_runtimes(access_point, False)
            self._persist_registry("start_agent_action", access_point)
        operation.phase = "followup"
        operation.fallback_reply = reply
        self._runtime.submit_request(access_point, build_action_result_prompt(action_results))

    def handle_driver_event(self, event: StewardDriverEvent) -> None:
        payload = event.event
        self._logger.event(
            "steward_driver_event_handling",
            **self._hooks.access_point_fields(event.access_point),
            route_target=event.source,
            event_type=type(payload).__name__,
        )
        if isinstance(payload, _TelegramStatusEvent):
            _output_runtime, status_store = self._access_point_adapter.status_runtime_for(
                access_point=event.access_point,
                source=self._kinds.agent_status_source if event.source == "agent" else self._kinds.steward_status_source,
            )
            status_store.apply_backend_status(
                status_text=payload.status_text,
                snapshot_getter=(
                    (lambda ap=event.access_point: self._agent_runtime.get_item_status_snapshot(ap))
                    if event.source == "agent"
                    else (lambda ap=event.access_point: self._runtime.get_item_status_snapshot(ap))
                ),
                snapshot_for_turn_getter=(
                    (lambda turn_id, ap=event.access_point: self._agent_runtime.get_item_status_snapshot_for_turn(ap, turn_id))
                    if event.source == "agent"
                    else (lambda turn_id, ap=event.access_point: self._runtime.get_item_status_snapshot_for_turn(ap, turn_id))
                ),
                apply_info_getter=(
                    (lambda ap=event.access_point: self._agent_runtime.get_last_item_apply_info(ap))
                    if event.source == "agent"
                    else (lambda ap=event.access_point: self._runtime.get_last_item_apply_info(ap))
                ),
            )
            return
        if isinstance(payload, _TelegramApprovalPromptEvent):
            self._logger.event(
                "steward_driver_approval_registered",
                **self._hooks.access_point_fields(event.access_point),
                route_target=event.source,
                req_id=payload.request.req_id,
            )
            self.register_approval(event, payload.request)
            return
        if isinstance(payload, _TelegramOutboundNote):
            self._access_point_adapter.send_outbound_note(
                access_point=event.access_point,
                source=event.source,
                text=payload.text,
            )
            return
        if isinstance(payload, _TelegramAgentResult):
            self._logger.event(
                "steward_driver_result_received",
                **self._hooks.access_point_fields(event.access_point),
                route_target=event.source,
                reply_preview=payload.reply.strip()[:200],
            )
            if event.source == "agent":
                operation = self._active_operations.get(event.access_point)
                if operation is not None and operation.source == "agent":
                    if payload.reply.startswith("agent error: "):
                        self._logger.event(
                            self._hooks.turn_error_event,
                            **self._hooks.access_point_fields(event.access_point),
                            route_target="agent",
                            error=payload.reply[len("agent error: "):],
                            error_type="RuntimeError",
                        )
                    operation.phase = "waiting_reply_delivery"
                    self._access_point_adapter.queue_text_reply(
                        access_point=event.access_point,
                        text=payload.reply,
                        source="agent",
                        kind=operation.output_kind,
                        on_sent=lambda ap=event.access_point, body=payload.reply: self._complete_reply_delivery(
                            access_point=ap,
                            source="agent",
                            text=body,
                        ),
                        on_failed=lambda exc, ap=event.access_point: self._fail_reply_delivery(
                            access_point=ap,
                            exc=exc,
                        ),
                    )
                return
            self.process_steward_result(event.access_point, payload)
            return
        raise AssertionError(f"unsupported steward driver event: {payload!r}")

    def _complete_reply_delivery(self, *, access_point: AccessPointKey, source: str, text: str) -> None:
        decorated = self._access_point_adapter.decorate_reply(text=text, source=source)
        self._logger.event(
            self._hooks.response_sent_event,
            **self._hooks.access_point_fields(access_point),
            text=decorated,
        )
        self._writer(self._hooks.write_output_line(access_point, text))
        self._active_operations.pop(access_point, None)

    def _fail_reply_delivery(self, *, access_point: AccessPointKey, exc: Exception) -> None:
        http_code = getattr(exc, "http_code", None)
        if http_code is None:
            http_code = extract_telegram_http_code(exc)
        self._logger.event(
            "telegram_send_error",
            access_point_type=access_point.type,
            chat_id=access_point.chat_id,
            thread_id=access_point.thread_id,
            error=str(exc),
            error_type=type(exc).__name__,
            http_code=http_code,
        )
        self._writer(
            f"telegram-steward send error chat_id={access_point.chat_id} "
            f"thread_id={access_point.thread_id}: {exc}"
        )
        self._active_operations.pop(access_point, None)

    def drain_driver_events(self) -> bool:
        progressed = False
        pending_statuses: dict[tuple[AccessPointKey, str], StewardDriverEvent] = {}

        def _flush_pending_statuses() -> None:
            nonlocal pending_statuses
            if not pending_statuses:
                return
            for key in sorted(
                pending_statuses.keys(),
                key=lambda value: (*access_point_sort_key(value[0]), value[1]),
            ):
                self.handle_driver_event(pending_statuses[key])
            pending_statuses = {}

        def _drain(events: list[StewardDriverEvent]) -> None:
            nonlocal progressed
            for event in events:
                progressed = True
                if isinstance(event.event, _TelegramStatusEvent):
                    pending_statuses[(event.access_point, event.source)] = event
                    continue
                _flush_pending_statuses()
                self.handle_driver_event(event)

        _drain(self._runtime.poll_once())
        _drain(self._agent_runtime.poll_once())
        _flush_pending_statuses()
        return progressed

    def handle_text_update(self, inbound: StewardInboundText) -> None:
        access_point = inbound.access_point
        self._logger.event(
            "steward_inbound_text_handling",
            **self._hooks.access_point_fields(access_point),
            text_preview=inbound.text.strip()[:200],
        )
        if hasattr(self._access_point_adapter, "drop_pending_status_updates"):
            self._access_point_adapter.drop_pending_status_updates(
                access_point=access_point,
                include_steward=True,
            )
        agent_state = self._agent_runtime.runtime_state(access_point)
        is_bound = agent_state != "UNBOUND"
        is_running_bound = agent_state == "RUNNING"
        fallback_cmd = extract_fallback_command(inbound.text)
        self._logger.event(
            "steward_inbound_state_snapshot",
            **self._hooks.access_point_fields(access_point),
            runtime_state=agent_state,
            is_bound=is_bound,
            is_running_bound=is_running_bound,
            fallback_cmd=fallback_cmd,
        )
        if fallback_cmd == "help":
            if self._hooks.on_fallback_command is not None:
                self._hooks.on_fallback_command(access_point, fallback_cmd, is_bound)
            self._queue_local_reply(
                access_point=access_point,
                text=self._access_point_adapter.build_help_text(state=agent_state),
                source="steward",
                kind=self._kinds.command,
            )
            return
        if fallback_cmd == "where":
            if self._hooks.on_fallback_command is not None:
                self._hooks.on_fallback_command(access_point, fallback_cmd, is_bound)
            self._queue_local_reply(
                access_point=access_point,
                text=self._access_point_adapter.build_where_text(access_point=access_point, state=agent_state),
                source="steward",
                kind=self._kinds.command,
            )
            return
        if fallback_cmd == "status":
            if self._hooks.on_fallback_command is not None:
                self._hooks.on_fallback_command(access_point, fallback_cmd, is_bound)
            self._queue_local_reply(
                access_point=access_point,
                text=self.build_status_text(access_point=access_point, state=agent_state),
                source="steward",
                kind=self._kinds.command,
            )
            return
        if fallback_cmd == "bind":
            if is_running_bound:
                reply = "runtime agent is already running for this access point."
            elif is_bound:
                reply = "access point is already bound (state: BOUND_IDLE). Use /start or /reset."
            else:
                started = self._agent_runtime.start_agent(
                    access_point,
                    {
                        "cwd": str(Path.cwd().resolve()),
                        "model": "gpt-5-codex",
                        "mode": "proxy",
                    },
                )
                self._clear_status_runtimes(access_point, False)
                lines = [
                    "runtime agent started and bound.",
                    f"agent_id: {started.get('agent_id', '')}",
                    f"cwd: {started.get('cwd', '')}",
                    f"mode: {started.get('mode', '')}",
                ]
                model = str(started.get("model", "")).strip()
                if model:
                    lines.append(f"model: {model}")
                reply = "\n".join(lines)
                self._persist_registry("bind_command", access_point)
            if self._hooks.on_fallback_command is not None:
                self._hooks.on_fallback_command(access_point, fallback_cmd, is_bound)
            self._queue_local_reply(
                access_point=access_point,
                text=reply,
                source="steward",
                kind=self._kinds.command,
            )
            return
        if fallback_cmd == "stop":
            if is_running_bound:
                stopped = self._agent_runtime.stop_agent(access_point)
                reply = "runtime agent stopped. state: BOUND_IDLE." if stopped else "runtime agent stop failed."
            elif is_bound:
                reply = "runtime agent is already stopped (state: BOUND_IDLE)."
            else:
                reply = "no bound runtime agent. Use /bind to create one."
            if self._hooks.on_fallback_command is not None:
                self._hooks.on_fallback_command(access_point, fallback_cmd, is_bound)
            self._queue_local_reply(
                access_point=access_point,
                text=reply,
                source="steward",
                kind=self._kinds.command,
            )
            return
        if fallback_cmd == "start":
            if is_running_bound:
                reply = "runtime agent is already running for this access point."
            elif is_bound:
                started = self._agent_runtime.start_bound_agent(access_point)
                self._clear_status_runtimes(access_point, False)
                lines = [
                    "runtime agent started.",
                    f"agent_id: {started.get('agent_id', '')}",
                    f"cwd: {started.get('cwd', '')}",
                    f"mode: {started.get('mode', '')}",
                ]
                model = str(started.get("model", "")).strip()
                if model:
                    lines.append(f"model: {model}")
                reply = "\n".join(lines)
                self._persist_registry("start_command", access_point)
            else:
                reply = "no bound runtime agent. Use /bind to create one."
            if self._hooks.on_fallback_command is not None:
                self._hooks.on_fallback_command(access_point, fallback_cmd, is_bound)
            self._queue_local_reply(
                access_point=access_point,
                text=reply,
                source="steward",
                kind=self._kinds.command,
            )
            return
        if fallback_cmd == "reset":
            runtime_reset = self._agent_runtime.reset(access_point)
            steward_reset = self._runtime.reset(access_point)
            self._clear_status_runtimes(access_point, True)
            self._pending_startup_notice_context.pop(access_point, None)
            if runtime_reset or steward_reset:
                reply = (
                    "access point reset completed.\n"
                    f"- runtime_agent: {'reset' if runtime_reset else 'no_binding'}\n"
                    f"- steward_node: {'reset' if steward_reset else 'not_started'}\n"
                    "- state: UNBOUND"
                )
            else:
                reply = "nothing to reset for this access point."
            if runtime_reset or steward_reset:
                self._drop_persisted_state(access_point)
            if self._hooks.on_fallback_command is not None:
                self._hooks.on_fallback_command(access_point, fallback_cmd, is_bound)
            self._queue_local_reply(
                access_point=access_point,
                text=reply,
                source="steward",
                kind=self._kinds.command,
            )
            return
        if is_bound:
            steward_once_payload = extract_steward_once_payload(inbound.text)
            if steward_once_payload is not None:
                if not steward_once_payload:
                    self._queue_local_reply(
                        access_point=access_point,
                        text="usage: /steward <message>",
                        source="steward",
                        kind=self._kinds.warning,
                    )
                    return
                if self._hooks.on_bound_route is not None:
                    self._hooks.on_bound_route(access_point, "steward_once")
                self._logger.event(
                    "steward_route_selected",
                    **self._hooks.access_point_fields(access_point),
                    route_target="steward_once",
                )
                self._runtime.submit_request(
                    access_point,
                    steward_once_payload,
                    context_note=self._consume_startup_notice_context(access_point),
                )
                self._active_operations[access_point] = PendingStewardOperation(
                    access_point=access_point,
                    source="steward",
                    output_kind=self._kinds.reply,
                )
                return
            if is_running_bound:
                if self._hooks.on_bound_route is not None:
                    self._hooks.on_bound_route(access_point, "runtime_agent")
                self._logger.event(
                    "steward_route_selected",
                    **self._hooks.access_point_fields(access_point),
                    route_target="runtime_agent",
                )
                self._agent_runtime.submit_request(access_point, inbound.text)
                self._active_operations[access_point] = PendingStewardOperation(
                    access_point=access_point,
                    source="agent",
                    output_kind=self._kinds.reply,
                )
                return
            if self._hooks.on_bound_route is not None:
                self._hooks.on_bound_route(access_point, "steward_while_bound_idle")
            self._logger.event(
                "steward_route_selected",
                **self._hooks.access_point_fields(access_point),
                route_target="steward_while_bound_idle",
            )
        else:
            self._logger.event(
                "steward_route_selected",
                **self._hooks.access_point_fields(access_point),
                route_target="steward_unbound",
            )
        self._runtime.submit_request(
            access_point,
            inbound.text,
            context_note=self._consume_startup_notice_context(access_point),
        )
        self._active_operations[access_point] = PendingStewardOperation(
            access_point=access_point,
            source="steward",
            output_kind=self._kinds.reply,
        )


def extract_steward_once_payload(raw_text: str) -> str | None:
    stripped = str(raw_text or "").strip()
    if not stripped.startswith("/"):
        return None
    head, _, tail = stripped.partition(" ")
    cmd = head.strip().lower()
    if cmd == "/steward" or cmd.startswith("/steward@"):
        return tail.strip()
    return None


def _format_restore_session_label(item: Any | None, *, fallback_session_id: str = "") -> str:
    if item is None:
        fallback = fallback_session_id.strip()
        return fallback or "not selected"
    session_id = str(getattr(item, "session_id", "") or "").strip()
    thread_name = str(getattr(item, "thread_name", "") or "").strip()
    timestamp = str(getattr(item, "last_activity_iso", "") or "").strip()
    label = thread_name or session_id or fallback_session_id.strip() or "not selected"
    if timestamp:
        return f"{label} — {timestamp}"
    return label


def extract_fallback_command(raw_text: str) -> str | None:
    stripped = str(raw_text or "").strip()
    if not stripped.startswith("/"):
        return None
    head = stripped.split(None, 1)[0].lower()
    if head == "/help" or head.startswith("/help@"):
        return "help"
    if head == "/where" or head.startswith("/where@"):
        return "where"
    if head == "/status" or head.startswith("/status@"):
        return "status"
    if head == "/bind" or head.startswith("/bind@"):
        return "bind"
    if head == "/start" or head.startswith("/start@"):
        return "start"
    if head == "/stop" or head.startswith("/stop@"):
        return "stop"
    if head == "/reset" or head.startswith("/reset@"):
        return "reset"
    return None
