"""Telegram multi-access-point bot wired to real Steward nodes via Hub."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Callable, Optional

from orchestrator.approval import ApprovalPolicy
from orchestrator.processes import LifecycleLogger
from orchestrator.routing_profile_control import InMemoryRoutingProfileControl
from orchestrator.steward_core import (
    StewardCore,
    StewardCoreHooks,
    StewardCoreKinds,
)
from orchestrator.steward_inbound import (
    StewardApprovalDecision,
    StewardApprovalDetailsRequest,
    StewardInboundText,
)
from orchestrator.steward_runtime_support import (
    InteractiveAgentRuntime,
    InteractiveStewardRuntime,
    StopStewardLoop,
    StewardTickLoop,
)
from orchestrator.steward_runner import (
    StewardTransportAdapter,
    build_steward_registry_context,
    run_steward_runtime,
)
from orchestrator.telegram_output_runtime import TelegramKind
from orchestrator.telegram_status import TelegramStatusConfig
from orchestrator.telegram_steward_access_point import (
    TelegramStewardAccessPointAdapter,
)
from orchestrator.telegram_steward_helpers import (
    AccessPointAgentRuntime,
    AccessPointKey,
    AccessPointStewardRuntime,
    AgentBackendFactory,
    AgentNodeFactory,
    RuntimeDriverFactory,
    StewardBackendFactory,
    StewardDriverFactory,
    access_point_sort_key,
    _PersistedAccessPointState,
    _default_agent_backend_factory,
    _default_backend_factory,
)
from orchestrator.telegram_bridge import (
    TelegramCallbackUpdate,
    TelegramChatState,
    TelegramTextUpdate,
    extract_telegram_http_code,
    process_telegram_updates,
)
from orchestrator.telegram_api_thread_driver import TelegramApiThreadDriver
from orchestrator.telegram_smoke import TelegramApi


Writer = Callable[[str], None]


class _TelegramStewardTransport(StewardTransportAdapter):
    stopping_message = "telegram-steward stopping gracefully"
    sigint_event_name = "telegram_steward_sigint"
    stopped_event_name = "telegram_steward_stopped"

    def __init__(
        self,
        *,
        client: TelegramApiThreadDriver,
        logger: LifecycleLogger,
        writer: Writer,
        max_poll_cycles: int | None,
        allowed_chat_ids: Optional[set[int]],
        offset_ref: list[int | None],
        chat_state: TelegramChatState,
        on_text_update: Callable[[TelegramTextUpdate], None],
        on_callback_update: Callable[[TelegramCallbackUpdate], None],
        agent_runtime: InteractiveAgentRuntime,
        runtime: InteractiveStewardRuntime,
    ) -> None:
        self._client = client
        self._logger = logger
        self._writer = writer
        self._max_poll_cycles = max_poll_cycles
        self._allowed_chat_ids = allowed_chat_ids
        self._offset_ref = offset_ref
        self._chat_state = chat_state
        self._on_text_update = on_text_update
        self._on_callback_update = on_callback_update
        self._agent_runtime = agent_runtime
        self._runtime = runtime
        self._handled_poll_errors = 0

    def consume_transport(self) -> bool:
        error = self._client.take_poll_error()
        if error is not None:
            if isinstance(error, (KeyboardInterrupt, SystemExit)):
                raise KeyboardInterrupt()
            self._handled_poll_errors += 1
            http_code = extract_telegram_http_code(error)
            self._logger.event(
                "telegram_poll_error",
                error=str(error),
                error_type=type(error).__name__,
                http_code=http_code,
            )
            self._writer(f"telegram-steward poll error: {error}")
            if http_code == 409:
                raise StopStewardLoop(0, drain=False)
            if self._max_poll_cycles is not None and self._handled_poll_errors >= self._max_poll_cycles:
                raise StopStewardLoop(0, drain=True)
            return False
        updates = self._client.drain_updates()
        if not updates:
            return False
        process_telegram_updates(
            updates=updates,
            logger=self._logger,
            source="telegram_steward",
            allowed_chat_ids=self._allowed_chat_ids,
            offset_ref=self._offset_ref,
            chat_state=self._chat_state,
            on_text_update=self._on_text_update,
            on_callback_update=self._on_callback_update,
        )
        return True

    def should_stop_after_tick(self) -> bool:
        return (
            self._max_poll_cycles is not None
            and self._client.poll_cycles >= self._max_poll_cycles
            and not self._client.has_pending_transport_events()
        )

    def drain_on_forced_stop(self, loop: StewardTickLoop) -> int:
        return loop.drain_until_idle()

    def sleep_sec(self) -> float:
        return 0.01

    def handle_stop(self, loop: StewardTickLoop, stop: StopStewardLoop) -> int:
        if stop.drain:
            return loop.drain_until_idle(consume_transport=False)
        return stop.code

    def handle_error(self, exc: Exception) -> int:
        self._logger.event("telegram_steward_error", error=str(exc), error_type=type(exc).__name__)
        self._writer(f"telegram-steward error: {exc}")
        raise exc

    def stop_resources(self) -> None:
        self._client.stop_polling()
        self._agent_runtime.close()
        self._runtime.close()
        self._client.stop()


def run_telegram_steward(
    token: str,
    log_path: str,
    poll_timeout_sec: int = 30,
    allowed_chat_ids: Optional[set[int]] = None,
    insecure_skip_verify: bool = False,
    telegram_api_base_url: str | None = None,
    api: Optional[TelegramApi] = None,
    writer: Writer = print,
    max_poll_cycles: Optional[int] = None,
    agent_command: Optional[list[str]] = None,
    request_timeout_sec: float = 0.0,
    rpc_timeout_sec: float = 5.0,
    rpc_retries: int = 3,
    approval_policy: Optional[ApprovalPolicy] = None,
    thread_approval_policy: str = "on-request",
    thread_sandbox: str = "workspace-write",
    transport_error_sleep_sec: float = 2.0,
    sessions_root: str | Path | None = None,
    steward_prompt: str | None = None,
    state_path: str | Path | None = None,
    steward_client_factory: Callable[..., Any] | None = None,
    runtime_client_factory: Callable[..., Any] | None = None,
    steward_driver_factory: StewardDriverFactory | None = None,
    runtime_driver_factory: RuntimeDriverFactory | None = None,
    telegram_status_config: TelegramStatusConfig | None = None,
    status_monotonic_now: Callable[[], float] | None = None,
) -> int:
    if not allowed_chat_ids:
        raise ValueError("allowed_chat_ids must be non-empty")
    logger = LifecycleLogger(Path(log_path))
    raw_client = api if api is not None else TelegramApi(
        token=token,
        insecure_skip_verify=insecure_skip_verify,
        api_base_url=telegram_api_base_url,
    )
    client = TelegramApiThreadDriver(raw_client)
    client.start()
    resolved_command = list(agent_command) if agent_command is not None else ["codex", "app-server"]
    if not resolved_command:
        raise ValueError("agent_command must not be empty")
    status_config = telegram_status_config or TelegramStatusConfig()
    resolved_policy = approval_policy or ApprovalPolicy.from_csv("", "", default_decision="human")
    runtime = InteractiveStewardRuntime(
        logger=logger,
        agent_command=resolved_command,
        request_timeout_sec=request_timeout_sec,
        rpc_timeout_sec=rpc_timeout_sec,
        rpc_retries=rpc_retries,
        approval_policy=resolved_policy,
        thread_approval_policy=thread_approval_policy,
        thread_sandbox=thread_sandbox,
        steward_prompt=steward_prompt,
        client_factory=steward_client_factory,
        driver_factory=steward_driver_factory,
    )
    agent_runtime = InteractiveAgentRuntime(
        logger=logger,
        agent_command=resolved_command,
        request_timeout_sec=request_timeout_sec,
        rpc_timeout_sec=rpc_timeout_sec,
        rpc_retries=rpc_retries,
        approval_policy=resolved_policy,
        default_thread_approval_policy=thread_approval_policy,
        default_thread_sandbox=thread_sandbox,
        client_factory=runtime_client_factory,
        driver_factory=runtime_driver_factory,
    )
    routing_profile_control = InMemoryRoutingProfileControl()
    registry = build_steward_registry_context(
        logger=logger,
        agent_runtime=agent_runtime,
        state_path=state_path,
        sort_key=access_point_sort_key,
    )

    chat_state = TelegramChatState()
    offset_ref: list[int | None] = [None]
    writer("telegram-steward started; incoming text is routed to per-access-point steward node")
    logger.event(
        "telegram_steward_started",
        poll_timeout_sec=poll_timeout_sec,
        allowed_chat_ids=sorted(allowed_chat_ids) if allowed_chat_ids is not None else None,
        mode="real_steward",
    )
    access_point_adapter = TelegramStewardAccessPointAdapter(
        client=client,
        logger=logger,
        writer=writer,
        status_config=status_config,
        status_monotonic_now=status_monotonic_now,
    )

    def _clear_status_runtimes(*, access_point: AccessPointKey, include_steward: bool = False) -> None:
        access_point_adapter.clear_status_runtimes(access_point=access_point, include_steward=include_steward)

    def _flush_due_status_runtimes() -> bool:
        return access_point_adapter.flush_due_status_runtimes()

    def _split_status_after_approval(access_point: AccessPointKey, _decision: str) -> None:
        access_point_adapter.split_status_after_approval(access_point)

    if registry.state_store is not None:
        registry.persist(reason="startup_restore")
    core = StewardCore(
        logger=logger,
        writer=writer,
        runtime=runtime,
        agent_runtime=agent_runtime,
        access_point_adapter=access_point_adapter,
        routing_profile_control=routing_profile_control,
        sessions_root=sessions_root,
        persisted_state_by_access_point=registry.persisted_state_by_access_point,
        pending_restore_greeting=registry.pending_restore_greeting,
        persist_registry=lambda reason, access_point=None: registry.persist(reason=reason, access_point=access_point),
        drop_persisted_state=registry.drop,
        clear_status_runtimes=lambda access_point, include_steward=False: _clear_status_runtimes(
            access_point=access_point,
            include_steward=include_steward,
        ),
        split_status_after_approval=_split_status_after_approval,
        kinds=StewardCoreKinds(
            reply=TelegramKind.REPLY,
            command=TelegramKind.COMMAND,
            warning=TelegramKind.WARNING,
            restore=TelegramKind.RESTORE,
            steward_status_source="steward",
            agent_status_source="agent",
        ),
        hooks=StewardCoreHooks(
            approval_human_response_event="telegram_approval_human_response",
            response_sent_event="telegram_steward_response_sent",
            turn_error_event="telegram_steward_turn_error",
            access_point_fields=lambda access_point: {
                "access_point_type": access_point.type,
                "chat_id": access_point.chat_id,
                "thread_id": access_point.thread_id,
            },
            write_output_line=lambda access_point, text: (
                f"chat_id={access_point.chat_id} "
                f"thread_id={access_point.thread_id} "
                f"out={text!r}"
            ),
            on_pre_approval_decision=access_point_adapter.clear_approval_markup,
            on_fallback_command=lambda access_point, command, bound: logger.event(
                "access_point_fallback_command_handled",
                access_point_type=access_point.type,
                chat_id=access_point.chat_id,
                thread_id=access_point.thread_id,
                command=command,
                bound=bound,
            ),
            on_bound_route=lambda access_point, route_target: logger.event(
                "access_point_bound_message_routed",
                access_point_type=access_point.type,
                chat_id=access_point.chat_id,
                thread_id=access_point.thread_id,
                route_target=route_target,
            ),
        ),
    )
    core.emit_startup_restore_notices()

    def _enqueue_text_update(update: TelegramTextUpdate) -> None:
        core.enqueue_inbound(access_point_adapter.inbound_text_from_update(update))
        logger.event(
            "telegram_text_update_enqueued",
            source="telegram_steward",
            update_id=update.update_id,
            chat_id=update.chat_id,
            thread_id=update.thread_id,
            queue_size=core.pending_queue_size,
        )

    def _handle_callback_update(update: TelegramCallbackUpdate) -> None:
        pending_approval = core.pending_approval_for(
            AccessPointKey(type="telegram", chat_id=update.chat_id, thread_id=update.thread_id)
        )
        action = access_point_adapter.approval_action_from_callback(
            update=update,
            pending_approval=pending_approval,
        )
        if action is None:
            return
        core.handle_approval_action(action)

    def _drain_pending_inputs() -> bool:
        return core.drain_pending_inputs(
            preprocess_inbound=lambda inbound: access_point_adapter.preprocess_inbound(
                inbound=inbound,
                pending_approval=core.pending_approval_for(inbound.access_point),
                handle_approval_action=core.handle_approval_action,
                handle_invalid_pending_approval_input=lambda: core.handle_invalid_pending_approval_input(inbound.access_point),
            )
        )

    transport = _TelegramStewardTransport(
        client=client,
        logger=logger,
        writer=writer,
        max_poll_cycles=max_poll_cycles,
        allowed_chat_ids=allowed_chat_ids,
        offset_ref=offset_ref,
        chat_state=chat_state,
        on_text_update=_enqueue_text_update,
        on_callback_update=_handle_callback_update,
        agent_runtime=agent_runtime,
        runtime=runtime,
    )

    loop = StewardTickLoop(
        request_timeout_sec=request_timeout_sec,
        drain_driver_events=core.drain_driver_events,
        flush_due_status_runtimes=_flush_due_status_runtimes,
        consume_transport=transport.consume_transport,
        drain_pending_inputs=_drain_pending_inputs,
        is_idle=lambda: core.is_idle() and not access_point_adapter.has_pending_outbound(),
    )

    loop.drain_until_idle(consume_transport=False)
    client.start_polling(
        offset=None,
        poll_timeout_sec=poll_timeout_sec,
        poll_error_backoff_sec=transport_error_sleep_sec,
    )
    return run_steward_runtime(
        loop=loop,
        transport=transport,
        writer=writer,
        logger=logger,
    )
