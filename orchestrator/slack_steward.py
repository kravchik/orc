"""Slack multi-access-point bot wired to real Steward nodes via shared steward runtime support."""

from __future__ import annotations

import time
from pathlib import Path
from typing import Any, Callable, Optional

from orchestrator.access_point_common import AccessPointKey, access_point_sort_key
from orchestrator.approval import ApprovalPolicy
from orchestrator.interactive_driver_events import RuntimeDriverFactory, StewardDriverFactory
from orchestrator.processes import LifecycleLogger
from orchestrator.slack_api_thread_driver import SlackApiThreadDriver
from orchestrator.slack_interactivity import SlackInboundSource, SlackInteractivityAction
from orchestrator.slack_smoke import SlackApi
from orchestrator.slack_steward_access_point import (
    SlackStewardAccessPointAdapter,
)
from orchestrator.steward_core import StewardCore, StewardCoreHooks, StewardCoreKinds
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
    run_steward_runtime,
)
from orchestrator.steward_state import build_steward_registry_context
from orchestrator.telegram_status import TelegramStatusConfig
from orchestrator.uploads import MAX_UPLOAD_FILE_BYTES, slack_uploads_from_message

Writer = Callable[[str], None]


class _SlackStewardTransport(StewardTransportAdapter):
    stopping_message = "slack-steward stopping gracefully"
    sigint_event_name = "slack_steward_sigint"
    stopped_event_name = "slack_steward_stopped"

    def __init__(
        self,
        *,
        client: SlackApiThreadDriver,
        logger: LifecycleLogger,
        writer: Writer,
        channel_id: str,
        bot_user_id: str | None,
        interactivity_source: SlackInboundSource | None,
        poll_interval_sec: float,
        max_poll_cycles: int | None,
        on_message: Callable[[dict[str, Any]], None],
        on_interactive_action: Callable[[SlackInteractivityAction], None],
        agent_runtime: InteractiveAgentRuntime,
        runtime: InteractiveStewardRuntime,
    ) -> None:
        self._client = client
        self._logger = logger
        self._writer = writer
        self._channel_id = channel_id
        self._bot_user_id = bot_user_id
        self._interactivity_source = interactivity_source
        self._poll_interval_sec = poll_interval_sec
        self._max_poll_cycles = max_poll_cycles
        self._on_message = on_message
        self._on_interactive_action = on_interactive_action
        self._agent_runtime = agent_runtime
        self._runtime = runtime
        self._poll_cycles = 0
        self._last_seen_ts: str | None = None

    def start(self) -> None:
        if self._interactivity_source is not None:
            self._interactivity_source.start()

    def consume_transport(self) -> bool:
        if self._interactivity_source is not None and self._interactivity_source.receives_messages():
            messages = self._interactivity_source.poll_messages(limit=20)
        else:
            if not self._channel_id:
                raise ValueError(
                    "slack-steward polling transport requires channel_id; "
                    "for multi-channel product use Socket Mode"
                )
            messages = self._client.conversations_history(
                channel_id=self._channel_id,
                oldest=self._last_seen_ts,
                limit=20,
            )
        filtered: list[dict[str, Any]] = []
        progressed = False
        for message in messages:
            ts = _normalized_ts(message.get("ts"))
            if ts is None:
                continue
            if self._last_seen_ts is not None and _ts_to_float(ts) <= _ts_to_float(self._last_seen_ts):
                continue
            if _is_bot_message(message=message, bot_user_id=self._bot_user_id):
                if self._last_seen_ts is None or _ts_to_float(ts) > _ts_to_float(self._last_seen_ts):
                    self._last_seen_ts = ts
                progressed = True
                continue
            message_channel = str(message.get("channel") or "").strip()
            if not message_channel:
                continue
            filtered.append(message)
        filtered.sort(key=lambda item: _ts_to_float(str(item.get("ts") or "0")))
        for message in filtered:
            ts = _normalized_ts(message.get("ts"))
            if ts is None:
                continue
            self._logger.event(
                "slack_steward_transport_message_received",
                channel_id=str(message.get("channel") or self._channel_id),
                thread_ts=_normalized_ts(message.get("thread_ts")) or ts,
                message_ts=ts,
                user=message.get("user"),
                text_preview=str(message.get("text") or "").strip()[:200],
            )
            self._on_message(message)
            if self._last_seen_ts is None or _ts_to_float(ts) > _ts_to_float(self._last_seen_ts):
                self._last_seen_ts = ts
            progressed = True
        if self._interactivity_source is not None:
            for action in self._interactivity_source.poll_actions(limit=20):
                self._logger.event(
                    "slack_steward_transport_action_received",
                    channel_id=action.channel_id,
                    thread_ts=action.thread_ts,
                    message_ts=action.message_ts,
                    action_id=action.action_id,
                    value=action.value,
                )
                self._on_interactive_action(action)
                progressed = True
        return progressed

    def should_stop_after_tick(self) -> bool:
        self._poll_cycles += 1
        return self._max_poll_cycles is not None and self._poll_cycles >= self._max_poll_cycles

    def drain_on_forced_stop(self, loop: StewardTickLoop) -> int:
        return loop.drain_until_idle()

    def sleep_sec(self) -> float:
        return max(0.0, float(self._poll_interval_sec))

    def handle_stop(self, loop: StewardTickLoop, stop: StopStewardLoop) -> int:
        if stop.drain:
            return loop.drain_until_idle()
        return stop.code

    def handle_error(self, exc: Exception) -> int:
        self._logger.event("slack_steward_error", error=str(exc), error_type=type(exc).__name__)
        self._writer(f"slack-steward error: {exc}")
        raise exc

    def stop_resources(self) -> None:
        if self._interactivity_source is not None:
            self._interactivity_source.stop()
        self._agent_runtime.close()
        self._runtime.close()
        self._client.stop()


def _normalized_ts(value: object) -> str | None:
    if not isinstance(value, str):
        return None
    text = value.strip()
    return text or None


def _ts_to_float(value: str) -> float:
    try:
        return float(value)
    except ValueError:
        return 0.0


def _is_bot_message(*, message: dict[str, Any], bot_user_id: str | None) -> bool:
    subtype = message.get("subtype")
    if isinstance(subtype, str) and subtype == "bot_message":
        return True
    bot_id = message.get("bot_id")
    if isinstance(bot_id, str) and bot_id.strip():
        return True
    if bot_user_id is None:
        return False
    user = message.get("user")
    return isinstance(user, str) and user == bot_user_id


def run_slack_steward(
    *,
    token: str,
    channel_id: str = "",
    log_path: str,
    slack_api_base_url: str | None = None,
    steward_command: list[str],
    agent_command: list[str],
    poll_interval_sec: float = 2.0,
    request_timeout_sec: float = 0.0,
    startup_timeout_sec: float = 120.0,
    rpc_timeout_sec: float = 5.0,
    rpc_retries: int = 3,
    api: Optional[SlackApi] = None,
    writer: Writer = print,
    max_poll_cycles: int | None = None,
    approval_policy: Optional[ApprovalPolicy] = None,
    thread_approval_policy: str = "on-request",
    thread_sandbox: str = "workspace-write",
    steward_prompt: str | None = None,
    steward_driver_factory: StewardDriverFactory | None = None,
    runtime_driver_factory: RuntimeDriverFactory | None = None,
    steward_client_factory: Callable[..., Any] | None = None,
    runtime_client_factory: Callable[..., Any] | None = None,
    state_path: str | None = "state/slack-steward-access-points.json",
    sessions_root: str | Path | None = None,
    slack_status_config: TelegramStatusConfig | None = None,
    interactivity_source: SlackInboundSource | None = None,
    consume_control_events: Callable[[], bool] | None = None,
    idle_sleep_fn: Callable[[float], None] = time.sleep,
    routing_queue_capacity: int = StewardCore._DEFAULT_ROUTING_QUEUE_CAPACITY,
) -> int:
    logger = LifecycleLogger(Path(log_path))
    raw_client = api if api is not None else SlackApi(token, api_base_url=slack_api_base_url)
    client = SlackApiThreadDriver(raw_client)
    client.start()
    resolved_policy = approval_policy or ApprovalPolicy.from_csv("", "", default_decision="human")
    status_config = slack_status_config or TelegramStatusConfig()
    runtime = InteractiveStewardRuntime(
        logger=logger,
        agent_command=list(steward_command),
        request_timeout_sec=request_timeout_sec,
        rpc_timeout_sec=rpc_timeout_sec,
        rpc_retries=rpc_retries,
        approval_policy=resolved_policy,
        thread_approval_policy=thread_approval_policy,
        thread_sandbox=thread_sandbox,
        steward_prompt=steward_prompt,
        client_factory=steward_client_factory,
        driver_factory=steward_driver_factory,
        edge_thread_name="slack-steward-jsonrpc-client-thread",
    )
    agent_runtime = InteractiveAgentRuntime(
        logger=logger,
        agent_command=list(agent_command),
        request_timeout_sec=request_timeout_sec,
        rpc_timeout_sec=rpc_timeout_sec,
        rpc_retries=rpc_retries,
        approval_policy=resolved_policy,
        default_thread_approval_policy=thread_approval_policy,
        default_thread_sandbox=thread_sandbox,
        client_factory=runtime_client_factory,
        driver_factory=runtime_driver_factory,
        edge_thread_name="slack-runtime-jsonrpc-client-thread",
        startup_timeout_sec=startup_timeout_sec,
    )
    registry = build_steward_registry_context(
        logger=logger,
        agent_runtime=agent_runtime,
        state_path=state_path,
        sort_key=access_point_sort_key,
    )

    bot_user_id: str | None = None
    writer("slack-steward started; incoming text is routed to per-access-point steward node")
    logger.event(
        "slack_steward_started",
        configured_channel_id=channel_id or None,
        poll_interval_sec=float(poll_interval_sec),
        request_timeout_sec=float(request_timeout_sec),
        mode="real_steward",
    )
    try:
        auth = client.auth_test()
        raw_user = auth.get("user_id")
        if isinstance(raw_user, str) and raw_user.strip():
            bot_user_id = raw_user.strip()
    except Exception as exc:
        logger.event("slack_auth_test_failed", error=str(exc), error_type=type(exc).__name__)
    access_point_adapter = SlackStewardAccessPointAdapter(
        client=client,
        logger=logger,
        status_config=status_config,
    )

    def _clear_status_runtimes(*, access_point: AccessPointKey, include_steward: bool = False) -> None:
        access_point_adapter.clear_status_runtimes(access_point=access_point, include_steward=include_steward)

    def _flush_due_status_runtimes() -> bool:
        return access_point_adapter.flush_due_status_runtimes()

    if registry.state_store is not None:
        registry.persist(reason="startup_restore")
    core = StewardCore(
        logger=logger,
        writer=writer,
        runtime=runtime,
        agent_runtime=agent_runtime,
        access_point_adapter=access_point_adapter,
        sessions_root=sessions_root,
        persisted_state_by_access_point=registry.persisted_state_by_access_point,
        persist_registry=lambda reason, access_point=None: registry.persist(reason=reason, access_point=access_point),
        drop_persisted_state=registry.drop,
        clear_status_runtimes=lambda access_point, include_steward=False: _clear_status_runtimes(
            access_point=access_point,
            include_steward=include_steward,
        ),
        split_status=access_point_adapter.split_status,
        kinds=StewardCoreKinds(
            reply="reply",
            command="command",
            session="session",
            warning="warning",
            steward_status_source="steward",
            agent_status_source="agent",
        ),
        hooks=StewardCoreHooks(
            approval_human_response_event="slack_approval_human_response",
            response_sent_event="slack_steward_response_sent",
            turn_error_event="slack_steward_turn_error",
            access_point_fields=lambda access_point: {
                "access_point_type": access_point.type,
                "channel_id": access_point.chat_id,
                "thread_ts": access_point.thread_id,
            },
            write_output_line=lambda access_point, text: (
                f"channel_id={access_point.chat_id} "
                f"thread_ts={access_point.thread_id} "
                f"out={text!r}"
            ),
            on_pre_approval_decision=access_point_adapter.clear_approval_blocks,
        ),
        routing_queue_capacity=routing_queue_capacity,
    )

    def _handle_message(message: dict[str, Any]) -> None:
        access_point = access_point_adapter.access_point_from_message(message)
        if access_point is None:
            return
        attachments = slack_uploads_from_message(message)
        for attachment in attachments:
            try:
                if not attachment.download_ref:
                    raise ValueError("slack file has no private download URL")
                core.validate_incoming_upload(
                    access_point,
                    declared_size=attachment.declared_size,
                )
                content = client.download_file(
                    attachment.download_ref,
                    max_bytes=MAX_UPLOAD_FILE_BYTES,
                )
                ordinal, saved, persisted = core.save_incoming_upload(
                    access_point,
                    preferred_name=attachment.preferred_name,
                    content=content,
                )
                reply = (
                    f"🧑‍✈️ Uploaded: {ordinal}. {saved.name}\n"
                    "It will be included with your next message."
                )
                if not persisted:
                    reply += "\nWarning: pending upload state was not persisted."
                access_point_adapter.send_outbound_note(
                    access_point=access_point,
                    source="steward",
                    text=reply,
                    kind="session",
                )
            except Exception as exc:
                logger.event(
                    "slack_upload_failed",
                    access_point_type=access_point.type,
                    channel_id=access_point.chat_id,
                    thread_ts=access_point.thread_id,
                    file_id=attachment.source_id,
                    file_name=attachment.preferred_name,
                    error=str(exc),
                    error_type=type(exc).__name__,
                )
                access_point_adapter.send_outbound_note(
                    access_point=access_point,
                    source="steward",
                    text=f"🧑‍✈️ Upload failed for {attachment.preferred_name}: {exc}",
                    kind="warning",
                )
        text = message.get("text")
        if not isinstance(text, str) or (attachments and not text.strip()):
            return
        inbound = StewardInboundText(access_point=access_point, text=text)
        logger.event(
            "slack_steward_inbound_enqueued",
            access_point_type=inbound.access_point.type,
            chat_id=inbound.access_point.chat_id,
            thread_id=inbound.access_point.thread_id,
            text_preview=inbound.text.strip()[:200],
        )
        core.enqueue_inbound(inbound)

    def _handle_interactive_action(action: SlackInteractivityAction) -> None:
        approval_action = None
        for pending_approval in core.iter_pending_approvals():
            approval_action = access_point_adapter.approval_action_from_interactive(
                action=action,
                pending_approval=pending_approval,
            )
            if approval_action is not None:
                break
        if approval_action is None:
            logger.event(
                "slack_steward_interactive_action_ignored",
                channel_id=action.channel_id,
                thread_ts=action.thread_ts,
                message_ts=action.message_ts,
                action_id=action.action_id,
                value=action.value,
            )
            return
        core.handle_approval_action(approval_action)

    def _drain_pending_inputs() -> bool:
        return core.drain_pending_inputs(
            preprocess_inbound=lambda inbound: access_point_adapter.preprocess_inbound(
                inbound=inbound,
                pending_approval=core.pending_approval_for(inbound.access_point),
                handle_approval_action=core.handle_approval_action,
                handle_invalid_pending_approval_input=lambda: core.handle_invalid_pending_approval_input(inbound.access_point),
            )
        )

    transport = _SlackStewardTransport(
        client=client,
        logger=logger,
        writer=writer,
        channel_id=channel_id,
        bot_user_id=bot_user_id,
        interactivity_source=interactivity_source,
        poll_interval_sec=poll_interval_sec,
        max_poll_cycles=max_poll_cycles,
        on_message=_handle_message,
        on_interactive_action=_handle_interactive_action,
        agent_runtime=agent_runtime,
        runtime=runtime,
    )
    loop = StewardTickLoop(
        request_timeout_sec=request_timeout_sec,
        drain_driver_events=core.drain_driver_events,
        tick_interactions=core.tick_interactions,
        flush_due_status_runtimes=_flush_due_status_runtimes,
        consume_transport=transport.consume_transport,
        drain_pending_inputs=_drain_pending_inputs,
        is_idle=core.is_idle,
        consume_control_events=consume_control_events,
        sleep_fn=idle_sleep_fn,
    )

    loop.drain_until_idle(consume_transport=False)
    if interactivity_source is not None:
        interactivity_source.start()

    try:
        return run_steward_runtime(
            loop=loop,
            transport=transport,
            writer=writer,
            logger=logger,
            sleep_fn=idle_sleep_fn,
        )
    finally:
        access_point_adapter.log_delivery_handles_closed()
