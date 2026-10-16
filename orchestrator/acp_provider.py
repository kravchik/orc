"""ACP provider facade, host lifecycle, and session-port ownership."""

from __future__ import annotations

from dataclasses import dataclass
from io import StringIO
from types import MappingProxyType

from orchestrator import clock
from orchestrator.agent_capabilities import (
    AgentCapability,
    AgentCapabilitySnapshot,
    acp_capability_snapshot as build_acp_capability_snapshot,
)
from orchestrator.agent_identity import (
    ProviderExecutionProfile,
    ProviderKind,
    ProviderProfileId,
    ProviderSessionRef,
    TurnRef,
)
from orchestrator.agent_inspection import (
    AgentBackgroundSnapshot,
    AgentConfigurationSnapshot,
    AgentContextSnapshot,
    AgentRuntimeProjection,
    AgentSessionSnapshot,
    AgentWorkSnapshot,
    SnapshotAvailability,
)
from orchestrator.agent_io import AgentOutput
from orchestrator.agent_provider import (
    ComposedProviderHost,
    EffectiveRuntimeSnapshot,
    OpenFailed,
    OpenSessionCorrelationId,
    OpenSessionError,
    OpenSessionErrorKind,
    OpenSessionOperation,
    OpenSessionRequest,
    Opened,
    ListProviderSessions,
    ProviderHostRootConfiguration,
    ProviderHostLifetime,
    ProviderRegistrationContext,
    ProviderRegistry,
    ProviderStartupFailureCategory,
    ProviderSessionCatalogEntry,
    ProviderSessionCatalogErrorKind,
    ProviderSessionsListFailed,
    ProviderSessionsListed,
    ProviderSessionsListResult,
    ResumeSessionRequest,
    StartSessionRequest,
    StopHost,
    StopHostResult,
)
from orchestrator.agent_session import (
    AgentApprovalRequest,
    AgentEffectResult,
    AgentEffectOutcome,
    AgentFormRequest,
    AgentSessionEffect,
    AgentSessionEvent,
    ApprovalDecisionResult,
    ApprovalRequested,
    CloseSession,
    ElicitationAction,
    ElicitationDecisionResult,
    ElicitationRequested,
    EffortSelectionAccepted,
    EffortSelectionFailed,
    EffortSelectionUncertain,
    InspectionCompleted,
    InterruptTurn,
    ModelSelectionAccepted,
    ModelSelectionFailed,
    ModelSelectionUncertain,
    OutputDelta,
    ProviderNotice,
    RawProviderErrorPayload,
    RefreshInspection,
    ResolveApproval,
    ResolveElicitation,
    SetEffort,
    SetModel,
    StartTurn,
    SessionFailed,
    TurnCompleted,
    TurnFailed,
    TurnInterrupted,
    TurnStarted,
    TrySteerTurn,
    approval_decision_preflight,
    unsupported_effect_result,
)
from orchestrator.runtime_profile_catalog import (
    ACP_ADAPTER_COMPONENT,
    ACP_ADAPTER_VERSION,
    AcpRuntimeProfileConfig,
)

from orchestrator.acp_protocol import (
    AcpProviderConfig,
    _AcpContractError,
    _ConfigSnapshot,
    _SessionUsage,
    _effective_from_snapshot,
    _prepare_prompt_blocks,
)
from orchestrator.acp_transport import (
    _AcpEdge,
    _ApprovalRequestedEvent,
    _CALLBACK_HANDOFF_STALE,
    _CALLBACK_HANDOFF_WRITTEN,
    _CallbackResponseHandoff,
    _CancelCommand,
    _CatalogFailed,
    _CloseFailed,
    _CloseSucceeded,
    _ConfigSelectionAcceptedEvent,
    _ConfigSelectionFailedEvent,
    _ConfigSelectionUncertainEvent,
    _ConfigSnapshotEvent,
    _ElicitationRequestedEvent,
    _HostFailed,
    _OpenCommand,
    _OpenFailedEvent,
    _OpenMetadata,
    _OpenSucceeded,
    _PromptCommand,
    _ProviderNoticeEvent,
    _ResolveFormCommand,
    _ResolvePermissionCommand,
    _SessionMetadataFailedEvent,
    _SetConfigCommand,
    _TurnCompletedEvent,
    _TurnFailedEvent,
    _TurnInterruptedEvent,
    _TurnOutputEvent,
    _UsageUpdatedEvent,
)

class AcpProviderHost:
    def __init__(self, config: AcpProviderConfig) -> None:
        self._config = config
        self._edge: _AcpEdge | None = None
        self._pending: dict[OpenSessionCorrelationId, _AcpOpenOperation] = {}
        self._ports: dict[str, AcpSessionAdapter] = {}
        self._closing_sessions: dict[str, _ClosingSession] = {}
        self._stopped = False
        self._terminal_error = ""
        self._started_logged = False

    @property
    def provider(self) -> ProviderKind:
        return self._config.runtime_profile.provider

    @property
    def profile(self) -> ProviderProfileId:
        return self._config.runtime_profile.profile

    def open_session(self, request: OpenSessionRequest) -> OpenSessionOperation:
        if not isinstance(request, (StartSessionRequest, ResumeSessionRequest)):
            raise ValueError("ACP host requires a typed open request")
        self._drain()
        if self._stopped:
            return _FailedOpenOperation(
                request,
                OpenSessionError(OpenSessionErrorKind.CANCELLED, "ACP host is stopped"),
            )
        if (
            isinstance(request, ResumeSessionRequest)
            and request.session_ref.execution_profile
            != ProviderExecutionProfile(self.provider, self.profile)
        ):
            return _FailedOpenOperation(
                request,
                OpenSessionError(
                    OpenSessionErrorKind.INVALID_REQUEST,
                    "ACP resume session belongs to a different provider profile",
                ),
            )
        edge = self._ensure_edge()
        operation = _AcpOpenOperation(self, request, edge)
        self._pending[operation.correlation_id] = operation
        try:
            operation.dispatch()
        except RuntimeError:
            self._pending.pop(operation.correlation_id, None)
            operation.fail(
                OpenSessionErrorKind.BUSY,
                "ACP startup admission capacity exceeded",
            )
        return operation

    def list_sessions(
        self,
        request: ListProviderSessions,
    ) -> ProviderSessionsListResult:
        if not isinstance(request, ListProviderSessions):
            raise ValueError("ACP session catalog requires ListProviderSessions")
        if request.execution_profile != ProviderExecutionProfile(
            self.provider,
            self.profile,
        ):
            return ProviderSessionsListFailed(
                ProviderSessionCatalogErrorKind.INVALID_REQUEST,
                "ACP session catalog request belongs to a different provider profile",
            )
        self._drain()
        if self._stopped or self._terminal_error:
            return ProviderSessionsListFailed(
                ProviderSessionCatalogErrorKind.UNAVAILABLE,
                "ACP session catalog is unavailable",
            )
        if request.limit == 0:
            return ProviderSessionsListed(())
        result = self._ensure_edge().list_sessions(request.cwd, request.limit)
        self._drain()
        if isinstance(result, _CatalogFailed):
            return ProviderSessionsListFailed(result.kind, result.error)
        return ProviderSessionsListed(
            tuple(
                ProviderSessionCatalogEntry(
                    session_ref=ProviderSessionRef(
                        self.provider,
                        entry.native_session_id,
                        self.profile,
                    ),
                    name=entry.name,
                    cwd=entry.cwd,
                    adapter_name=ACP_ADAPTER_COMPONENT,
                    adapter_version=ACP_ADAPTER_VERSION,
                    created_at=entry.created_at,
                    updated_at=entry.updated_at,
                    preview=entry.preview,
                )
                for entry in result.entries
            )
        )

    def _ensure_edge(self) -> _AcpEdge:
        edge = self._edge
        if edge is None:
            edge = _AcpEdge(self._config)
            self._edge = edge
            edge.start()
        return edge

    def consume_poll_progress(self) -> bool:
        edge = self._edge
        if edge is None:
            return False
        progressed = edge.consume_progress()
        self._drain()
        return progressed

    def consume_terminal_error(self) -> str:
        error = self._terminal_error
        self._terminal_error = ""
        edge_error = self._edge.consume_terminal_error() if self._edge is not None else ""
        return error or edge_error

    def stop_host(self, effect: StopHost) -> StopHostResult:
        if not isinstance(effect, StopHost):
            raise ValueError("ACP host stop requires StopHost")
        if self._stopped:
            return StopHostResult(stopped=True)
        self._drain()
        for operation in tuple(self._pending.values()):
            operation.cancel("ACP host stopped during session open")
        for port in tuple(self._ports.values()):
            port.close_session(CloseSession())
        edge = self._edge
        if edge is not None:
            try:
                edge.stop()
            except Exception as exc:
                return StopHostResult(stopped=False, error=str(exc))
            self._drain()
        for port in tuple(self._ports.values()):
            port._mark_closed()
        self._pending.clear()
        self._ports.clear()
        self._closing_sessions.clear()
        self._edge = None
        self._stopped = True
        self._config.logger.event(
            "agent_backend_stopped",
            provider=self.provider.value,
            profile=self.profile.value,
            role=self._config.role,
        )
        return StopHostResult(stopped=True)

    def _drain(self) -> None:
        edge = self._edge
        if edge is None:
            return
        for event in edge.poll_events():
            if isinstance(event, _HostFailed):
                for operation in tuple(self._pending.values()):
                    operation.fail(
                        OpenSessionErrorKind.STARTUP_FAILED,
                        event.error,
                        startup_failure_category=event.startup_failure_category,
                    )
                self._pending.clear()
                failed_ports = tuple(self._ports.values())
                for port in failed_ports:
                    port._receive_host_failure(event.error)
                self._ports.clear()
                self._closing_sessions.clear()
                if edge is self._edge:
                    try:
                        edge.stop()
                    except Exception:
                        self._terminal_error = "ACP host cleanup failed"
                    else:
                        self._edge = None
                return
            if isinstance(event, _TurnOutputEvent):
                port = self._ports.get(event.native_session_id)
                if port is not None:
                    port._receive_turn_output(event.turn_ref, event.text)
                continue
            if isinstance(event, _TurnCompletedEvent):
                port = self._ports.get(event.native_session_id)
                if port is not None:
                    port._receive_turn_completed(event.turn_ref)
                continue
            if isinstance(event, _TurnInterruptedEvent):
                port = self._ports.get(event.native_session_id)
                if port is not None:
                    port._receive_turn_interrupted(event.turn_ref)
                continue
            if isinstance(event, _TurnFailedEvent):
                port = self._ports.get(event.native_session_id)
                if port is not None:
                    port._receive_turn_failed(
                        event.turn_ref,
                        event.error,
                        raw_provider_error=event.raw_provider_error,
                        fatal=event.fatal,
                    )
                continue
            if isinstance(event, _ApprovalRequestedEvent):
                port = self._ports.get(event.native_session_id)
                if port is not None:
                    port._receive_approval_requested(event.turn_ref, event.request)
                continue
            if isinstance(event, _ProviderNoticeEvent):
                port = self._ports.get(event.native_session_id)
                if port is not None:
                    port._receive_provider_notice(event.turn_ref, event.text)
                continue
            if isinstance(event, _ElicitationRequestedEvent):
                port = self._ports.get(event.native_session_id)
                if port is not None:
                    port._receive_elicitation_requested(event.turn_ref, event.request)
                continue
            if isinstance(event, _ConfigSnapshotEvent):
                port = self._ports.get(event.native_session_id)
                if port is not None:
                    port._receive_config_snapshot(event.snapshot)
                continue
            if isinstance(event, _UsageUpdatedEvent):
                port = self._ports.get(event.native_session_id)
                if port is not None:
                    port._receive_usage(event.usage)
                continue
            if isinstance(event, _ConfigSelectionAcceptedEvent):
                port = self._ports.get(event.native_session_id)
                if port is not None:
                    port._receive_config_selection_accepted(
                        event.category,
                        event.requested_value,
                        event.snapshot,
                    )
                continue
            if isinstance(event, _ConfigSelectionFailedEvent):
                port = self._ports.get(event.native_session_id)
                if port is not None:
                    port._receive_config_selection_failed(
                        event.category, event.requested_value, event.error
                    )
                continue
            if isinstance(event, _ConfigSelectionUncertainEvent):
                port = self._ports.get(event.native_session_id)
                if port is not None:
                    port._receive_config_selection_uncertain(
                        event.category, event.requested_value, event.error
                    )
                continue
            if isinstance(event, _SessionMetadataFailedEvent):
                port = self._ports.get(event.native_session_id)
                if port is not None:
                    port._receive_metadata_failure(event.error)
                continue
            if isinstance(event, _CloseSucceeded):
                port = self._ports.pop(event.native_session_id, None)
                self._closing_sessions.pop(event.native_session_id, None)
                if port is not None:
                    turn_ref = port._closing_turn_ref()
                    if turn_ref is not None:
                        edge.abandon_turn(event.native_session_id, turn_ref)
                    port._mark_closed()
                    self._config.logger.event(
                        "acp_session_closed",
                        provider=self.provider.value,
                        profile=self.profile.value,
                    )
                continue
            if isinstance(event, _CloseFailed):
                closing = self._closing_sessions.get(event.native_session_id)
                if closing is not None:
                    closing.in_flight = False
                    closing.error = event.error
                    closing.failure_reported = False
                    if not closing.retry_logged:
                        closing.retry_logged = True
                        self._config.logger.event(
                            "acp_session_close_retry",
                            provider=self.provider.value,
                            profile=self.profile.value,
                            error=event.error,
                        )
                continue
            operation = self._pending.pop(event.correlation_id, None)
            try:
                if operation is None or operation.completed:
                    if isinstance(event, _OpenSucceeded):
                        edge.close_session(event.native_session_id)
                    continue
                if isinstance(event, _OpenFailedEvent):
                    operation.fail(event.kind, event.error)
                    continue
                if event.native_session_id in self._ports:
                    self._config.logger.event(
                        "provider_session_identity_collision",
                        provider=self.provider.value,
                        profile=self.profile.value,
                        correlation_id=operation.correlation_id.value,
                    )
                    operation.fail(
                        OpenSessionErrorKind.STARTUP_FAILED,
                        "ACP session identity collision",
                    )
                    continue
                operation.succeed(event.native_session_id, event.metadata)
            finally:
                edge.acknowledge_open(event.correlation_id)

    def _finish_open(
        self,
        operation: _AcpOpenOperation,
        native_session_id: str,
        metadata: _OpenMetadata,
    ) -> Opened:
        session_ref = ProviderSessionRef(self.provider, native_session_id, self.profile)
        capabilities = acp_capability_snapshot(
            self._config.runtime_profile,
            runtime_version=metadata.runtime_version,
        )
        port = AcpSessionAdapter(
            self,
            session_ref,
            capabilities,
            model=metadata.model,
            backend=metadata.backend,
            config_snapshot=metadata.config_snapshot,
            usage=metadata.usage,
            loaded=metadata.loaded,
            auth_method_ids=self._config.runtime_profile.auth_method_ids,
            turn_output_max_chars=self._config.turn_output_max_chars,
            event_capacity=self._config.port_event_capacity,
            workspace=operation.request.config.cwd,
        )
        self._ports[native_session_id] = port
        effective = EffectiveRuntimeSnapshot(
            model=metadata.model,
            backend=metadata.backend,
        )
        self._config.logger.event(
            "agent_capability_snapshot",
            provider=self.provider.value,
            profile=self.profile.value,
            **capabilities.to_log_fields(),
        )
        if not self._started_logged:
            self._started_logged = True
            self._config.logger.event(
                "agent_backend_started",
                provider=self.provider.value,
                profile=self.profile.value,
                role=self._config.role,
                runtime_version=metadata.runtime_version,
            )
        self._config.logger.event(
            "provider_session_opened",
            provider=self.provider.value,
            profile=self.profile.value,
            correlation_id=operation.correlation_id.value,
            effective_model=effective.model,
            effective_backend=effective.backend,
            replay_update_count=metadata.replay_update_count,
        )
        return Opened(
            operation.correlation_id,
            port,
            session_ref,
            effective,
            capabilities,
        )

    def _close_port(self, port: AcpSessionAdapter) -> AgentEffectResult:
        self._drain()
        native_id = port.session_ref.native_id
        if self._ports.get(native_id) is not port:
            port._mark_closed()
            return AgentEffectResult.completed()
        closing = self._closing_sessions.setdefault(native_id, _ClosingSession())
        if closing.in_flight:
            return AgentEffectResult.accepted()
        if closing.error and not closing.failure_reported:
            closing.failure_reported = True
            return AgentEffectResult.uncertain(closing.error)
        if closing.failure_reported:
            closing.error = ""
            closing.failure_reported = False
        edge = self._edge
        if edge is None:
            return AgentEffectResult.uncertain(
                "ACP session cleanup transport is unavailable"
            )
        try:
            edge.close_session(native_id)
        except RuntimeError:
            closing.error = "ACP session cleanup was not accepted"
            closing.failure_reported = True
            return AgentEffectResult.uncertain(closing.error)
        closing.in_flight = True
        closing.error = ""
        return AgentEffectResult.accepted()

    def _start_turn(
        self,
        port: AcpSessionAdapter,
        effect: StartTurn,
    ) -> AgentEffectResult:
        native_id = port.session_ref.native_id
        if self._ports.get(native_id) is not port or port._is_closing():
            return AgentEffectResult.rejected("ACP session is unavailable")
        edge = self._edge
        if edge is None:
            return AgentEffectResult.rejected("ACP transport is unavailable")
        try:
            prompt = _prepare_prompt_blocks(
                effect.agent_input,
                workspace=port._workspace,
                negotiated=edge.prompt_capabilities(),
            )
            edge.submit_turn(
                _PromptCommand(
                    native_session_id=native_id,
                    turn_ref=effect.turn_ref,
                    prompt=prompt,
                    workspace=port._workspace,
                )
            )
        except _AcpContractError as exc:
            return AgentEffectResult.rejected(str(exc))
        except RuntimeError:
            return AgentEffectResult.rejected("ACP turn admission is busy")
        return AgentEffectResult.accepted()

    def _set_config(
        self,
        port: AcpSessionAdapter,
        *,
        category: str,
        config_id: str,
        value: str,
    ) -> AgentEffectResult:
        native_id = port.session_ref.native_id
        if self._ports.get(native_id) is not port or port._is_closing():
            return AgentEffectResult.rejected("ACP session is unavailable")
        edge = self._edge
        if edge is None:
            return AgentEffectResult.rejected("ACP transport is unavailable")
        try:
            edge.submit_config_selection(
                _SetConfigCommand(native_id, category, config_id, value)
            )
        except RuntimeError:
            return AgentEffectResult.rejected("ACP configuration admission is busy")
        except _AcpContractError:
            port._receive_config_selection_uncertain(
                category,
                value,
                "ACP configuration selection outcome is uncertain",
            )
            return AgentEffectResult.uncertain(
                "ACP configuration selection outcome is uncertain"
            )
        return AgentEffectResult.accepted()

    def _abandon_turn(
        self,
        port: AcpSessionAdapter,
        turn_ref: TurnRef,
    ) -> None:
        native_id = port.session_ref.native_id
        if self._ports.get(native_id) is port and self._edge is not None:
            self._edge.abandon_turn(native_id, turn_ref)

    def _interrupt_turn(
        self,
        port: AcpSessionAdapter,
        effect: InterruptTurn,
    ) -> AgentEffectResult:
        native_id = port.session_ref.native_id
        if self._ports.get(native_id) is not port or port._is_closing():
            return AgentEffectResult.rejected("ACP session is unavailable")
        edge = self._edge
        if edge is None:
            return AgentEffectResult.rejected("ACP transport is unavailable")
        try:
            edge.submit_interrupt(_CancelCommand(native_id, effect.turn_ref))
        except RuntimeError:
            return AgentEffectResult.rejected("ACP interrupt admission is busy")
        return AgentEffectResult.accepted()

    def _resolve_approval(
        self,
        port: AcpSessionAdapter,
        effect: ResolveApproval,
    ) -> ApprovalDecisionResult:
        native_id = port.session_ref.native_id
        if self._ports.get(native_id) is not port or port._is_closing():
            return ApprovalDecisionResult.preflight(
                AgentEffectResult.rejected("ACP session is unavailable")
            )
        edge = self._edge
        if edge is None:
            return ApprovalDecisionResult.preflight(
                AgentEffectResult.rejected("ACP transport is unavailable")
            )
        handoff = _CallbackResponseHandoff()
        try:
            edge.submit_permission(
                _ResolvePermissionCommand(
                    native_id,
                    effect.turn_ref,
                    effect.approval_ref,
                    effect.decision,
                    handoff,
                )
            )
        except RuntimeError:
            return ApprovalDecisionResult.preflight(
                AgentEffectResult.rejected("ACP permission response admission is busy")
            )
        status = handoff.wait(self._config.rpc_timeout_sec)
        if status == _CALLBACK_HANDOFF_WRITTEN:
            return ApprovalDecisionResult.provider_call(AgentEffectResult.accepted())
        if status == _CALLBACK_HANDOFF_STALE:
            return ApprovalDecisionResult.preflight(
                AgentEffectResult.rejected("ACP permission request is stale")
            )
        return ApprovalDecisionResult.provider_call(
            AgentEffectResult.uncertain("ACP permission response outcome is uncertain")
        )

    def _resolve_elicitation(
        self,
        port: AcpSessionAdapter,
        effect: ResolveElicitation,
    ) -> ElicitationDecisionResult:
        native_id = port.session_ref.native_id
        if self._ports.get(native_id) is not port or port._is_closing():
            return ApprovalDecisionResult.preflight(
                AgentEffectResult.rejected("ACP session is unavailable")
            )
        edge = self._edge
        if edge is None:
            return ApprovalDecisionResult.preflight(
                AgentEffectResult.rejected("ACP transport is unavailable")
            )
        handoff = _CallbackResponseHandoff()
        try:
            edge.submit_form(
                _ResolveFormCommand(
                    native_id,
                    effect.turn_ref,
                    effect.elicitation_ref,
                    effect.action,
                    tuple(effect.content),
                    handoff,
                )
            )
        except RuntimeError:
            return ApprovalDecisionResult.preflight(
                AgentEffectResult.rejected("ACP elicitation response admission is busy")
            )
        status = handoff.wait(self._config.rpc_timeout_sec)
        if status == _CALLBACK_HANDOFF_WRITTEN:
            return ApprovalDecisionResult.provider_call(AgentEffectResult.accepted())
        if status == _CALLBACK_HANDOFF_STALE:
            return ApprovalDecisionResult.preflight(
                AgentEffectResult.rejected("ACP elicitation request is stale")
            )
        return ApprovalDecisionResult.provider_call(
            AgentEffectResult.uncertain("ACP elicitation response outcome is uncertain")
        )


@dataclass
class _ClosingSession:
    in_flight: bool = False
    error: str = ""
    failure_reported: bool = False
    retry_logged: bool = False


class _AcpOpenOperation(OpenSessionOperation):
    def __init__(
        self,
        host: AcpProviderHost,
        request: OpenSessionRequest,
        edge: _AcpEdge,
    ) -> None:
        super().__init__(request)
        self._host = host
        self._edge = edge
        self._deadline = clock.monotonic() + host._config.startup_timeout_sec
        self._dispatched = False

    def dispatch(self) -> None:
        self._dispatched = True
        self._publish_progress(phase="runtime_connecting", message="connecting ACP")
        self._edge.submit_open(
            _OpenCommand(
                self.correlation_id,
                self.request.config.cwd,
                (
                    self.request.session_ref.native_id
                    if isinstance(self.request, ResumeSessionRequest)
                    else ""
                ),
                self.request.config.model,
            )
        )

    def cancel(self, reason: str) -> None:
        if self.completed:
            return
        self._edge.cancel_open(self.correlation_id)
        self._host._pending.pop(self.correlation_id, None)
        self._complete(
            OpenFailed(
                self.correlation_id,
                OpenSessionError(OpenSessionErrorKind.CANCELLED, reason),
            )
        )

    def fail(
        self,
        kind: OpenSessionErrorKind,
        error: str,
        *,
        startup_failure_category: ProviderStartupFailureCategory | None = None,
    ) -> None:
        if not self.completed:
            self._complete(
                OpenFailed(
                    self.correlation_id,
                    OpenSessionError(
                        kind,
                        error,
                        startup_failure_category=startup_failure_category,
                    ),
                )
            )

    def succeed(self, native_session_id: str, metadata: _OpenMetadata) -> None:
        if not self.completed:
            self._complete(self._host._finish_open(self, native_session_id, metadata))

    def _advance(self) -> None:
        if not self._dispatched:
            return
        if clock.monotonic() >= self._deadline:
            self._edge.cancel_open(self.correlation_id)
            self._host._pending.pop(self.correlation_id, None)
            self.fail(OpenSessionErrorKind.STARTUP_TIMEOUT, "ACP startup timed out")
            return
        self._host._drain()


class _FailedOpenOperation(OpenSessionOperation):
    def __init__(self, request: OpenSessionRequest, error: OpenSessionError) -> None:
        super().__init__(request)
        self._complete(OpenFailed(self.correlation_id, error))

    def _advance(self) -> None:
        return


class AcpSessionAdapter:
    def __init__(
        self,
        host: AcpProviderHost,
        session_ref: ProviderSessionRef,
        capabilities: AgentCapabilitySnapshot,
        *,
        model: str,
        backend: str,
        config_snapshot: _ConfigSnapshot,
        usage: _SessionUsage | None,
        loaded: bool,
        auth_method_ids: tuple[str, ...],
        turn_output_max_chars: int,
        event_capacity: int,
        workspace: str,
    ) -> None:
        self._host = host
        self.session_ref = session_ref
        self._capabilities = capabilities
        self._model = model
        self._backend = backend
        self._config_snapshot = config_snapshot
        self._usage = usage
        self._loaded = loaded
        self._auth_method_ids = auth_method_ids
        self._turn_output_max_chars = turn_output_max_chars
        self._event_capacity = event_capacity
        self._workspace = workspace
        self._closed = False
        self._closing = False
        self._failure_emitted = False
        self._active_turn_ref: TurnRef | None = None
        self._interrupt_requested_ref: TurnRef | None = None
        self._active_approval_request: AgentApprovalRequest | None = None
        self._active_form_request: AgentFormRequest | None = None
        self._pending_config: tuple[str, str] | None = None
        self._usage_blocked_after_cancel = False
        self._pending_usage_after_cancel: _SessionUsage | None = None
        self._output_text = StringIO()
        self._events: list[AgentSessionEvent] = []

    def get_capability_snapshot(self) -> AgentCapabilitySnapshot:
        return self._capabilities

    def start_turn(self, effect: StartTurn) -> AgentEffectResult:
        if not isinstance(effect, StartTurn):
            raise ValueError("ACP turn start requires StartTurn")
        unsupported = unsupported_effect_result(effect, self._capabilities)
        if unsupported is not None:
            return unsupported
        if self._closed:
            return AgentEffectResult.rejected("ACP session is closed")
        if self._closing:
            return AgentEffectResult.rejected("ACP session is closing")
        if self._failure_emitted:
            return AgentEffectResult.rejected("ACP session is unavailable")
        if self._active_turn_ref is not None:
            return AgentEffectResult.rejected("ACP session is busy")
        if effect.agent_input.attachments and not self._capabilities.supports(
            AgentCapability.STRUCTURED_USER_INPUT
        ):
            return AgentEffectResult.unsupported(AgentCapability.STRUCTURED_USER_INPUT)
        result = self._host._start_turn(self, effect)
        if result.outcome is AgentEffectOutcome.ACCEPTED:
            self._active_turn_ref = effect.turn_ref
            if self._usage_blocked_after_cancel:
                self._pending_usage_after_cancel = None
            self._output_text = StringIO()
            self._queue_turn_event(TurnStarted(effect.turn_ref), effect.turn_ref)
        return result

    def try_steer_turn(self, effect: TrySteerTurn) -> AgentEffectResult:
        return self._unsupported(effect)

    def interrupt_turn(self, effect: InterruptTurn) -> AgentEffectResult:
        if not isinstance(effect, InterruptTurn):
            raise ValueError("ACP interrupt requires InterruptTurn")
        unsupported = unsupported_effect_result(effect, self._capabilities)
        if unsupported is not None:
            return unsupported
        if self._closed:
            return AgentEffectResult.rejected("ACP session is closed")
        if self._closing:
            return AgentEffectResult.rejected("ACP session is closing")
        if self._failure_emitted:
            return AgentEffectResult.rejected("ACP session is unavailable")
        if self._active_turn_ref != effect.turn_ref:
            return AgentEffectResult.rejected("ACP interrupt target is not active")
        if self._interrupt_requested_ref is not None:
            return AgentEffectResult.rejected("ACP interrupt is already requested")
        result = self._host._interrupt_turn(self, effect)
        if result.outcome is AgentEffectOutcome.ACCEPTED:
            self._interrupt_requested_ref = effect.turn_ref
            self._usage = None
            self._usage_blocked_after_cancel = True
            self._pending_usage_after_cancel = None
        return result

    def resolve_approval(self, effect: ResolveApproval) -> ApprovalDecisionResult:
        if not isinstance(effect, ResolveApproval):
            raise ValueError("ACP approval resolution requires ResolveApproval")
        request = self._active_approval_request
        if request is None:
            return ApprovalDecisionResult.preflight(
                AgentEffectResult.rejected("no active ACP permission request")
            )
        preflight = approval_decision_preflight(
            request,
            active_turn_ref=self._active_turn_ref,
            active_approval_ref=request.approval_ref,
            decision=effect.decision,
        )
        if (
            preflight.outcome is not AgentEffectOutcome.ACCEPTED
            or effect.turn_ref != request.turn_ref
            or effect.approval_ref != request.approval_ref
        ):
            return ApprovalDecisionResult.preflight(
                preflight
                if preflight.outcome is not AgentEffectOutcome.ACCEPTED
                else AgentEffectResult.rejected("ACP permission request is stale")
            )
        result = self._host._resolve_approval(self, effect)
        if result.delivery_attempted and result.succeeded:
            self._active_approval_request = None
        return result

    def resolve_elicitation(
        self,
        effect: ResolveElicitation,
    ) -> ElicitationDecisionResult:
        if not isinstance(effect, ResolveElicitation):
            raise ValueError("ACP elicitation resolution requires ResolveElicitation")
        request = self._active_form_request
        if (
            request is None
            or self._active_turn_ref != effect.turn_ref
            or request.turn_ref != effect.turn_ref
            or request.elicitation_ref != effect.elicitation_ref
        ):
            return ApprovalDecisionResult.preflight(
                AgentEffectResult.rejected("ACP elicitation request is stale")
            )
        if effect.action is ElicitationAction.ACCEPT:
            try:
                request.validate_content(dict(effect.content))
            except ValueError as exc:
                return ApprovalDecisionResult.preflight(
                    AgentEffectResult.rejected(str(exc))
                )
        result = self._host._resolve_elicitation(self, effect)
        if result.delivery_attempted and result.succeeded:
            self._active_form_request = None
        return result

    def set_model(self, effect: SetModel) -> AgentEffectResult:
        if not isinstance(effect, SetModel):
            raise ValueError("ACP model selection requires SetModel")
        return self._set_config_value("model", effect.model, effect)

    def set_effort(self, effect: SetEffort) -> AgentEffectResult:
        if not isinstance(effect, SetEffort):
            raise ValueError("ACP effort selection requires SetEffort")
        return self._set_config_value("thought_level", effect.effort, effect)

    def _set_config_value(
        self,
        category: str,
        value: str,
        effect: SetModel | SetEffort,
    ) -> AgentEffectResult:
        unsupported = unsupported_effect_result(effect, self._capabilities)
        if unsupported is not None:
            return unsupported
        if self._closed or self._closing:
            return AgentEffectResult.rejected("ACP session is unavailable")
        if self._failure_emitted:
            return AgentEffectResult.rejected("ACP session has failed")
        if self._active_turn_ref is not None:
            return AgentEffectResult.rejected("ACP session is busy")
        if self._pending_config is not None:
            return AgentEffectResult.rejected("ACP configuration selection is pending")
        option = self._config_snapshot.by_category(category)
        if (
            option is None
            or option.option_type != "select"
            or value not in option.values
        ):
            return AgentEffectResult.rejected("ACP requested configuration is unavailable")
        result = self._host._set_config(
            self,
            category=category,
            config_id=option.config_id,
            value=value,
        )
        if result.outcome is AgentEffectOutcome.ACCEPTED:
            self._pending_config = (category, value)
        return result

    def close_session(self, effect: CloseSession) -> AgentEffectResult:
        if not isinstance(effect, CloseSession):
            raise ValueError("ACP close requires CloseSession")
        if self._closed:
            return AgentEffectResult.completed()
        self._closing = True
        return self._host._close_port(self)

    def _mark_closed(self) -> None:
        self._closed = True
        self._closing = False
        self._active_turn_ref = None
        self._interrupt_requested_ref = None
        self._active_approval_request = None
        self._active_form_request = None
        self._pending_config = None
        self._usage = None
        self._pending_usage_after_cancel = None
        self._output_text = StringIO()
        self._events.clear()

    def _is_closing(self) -> bool:
        return self._closing

    def _closing_turn_ref(self) -> TurnRef | None:
        return self._active_turn_ref

    def refresh_inspection(self, effect: RefreshInspection) -> AgentEffectResult:
        if not isinstance(effect, RefreshInspection):
            raise ValueError("ACP inspection requires RefreshInspection")
        if self._closed:
            return AgentEffectResult.rejected("ACP provider session is closed")
        if len(self._events) >= self._event_capacity:
            return AgentEffectResult.rejected("ACP session event capacity exceeded")
        self._events.append(
            InspectionCompleted(effect.inspection_ref, self.get_local_projection())
        )
        return AgentEffectResult.accepted()

    def get_local_projection(self) -> AgentRuntimeProjection:
        model_option = self._config_snapshot.by_category("model")
        effort_option = self._config_snapshot.by_category("thought_level")
        configuration_availability = (
            SnapshotAvailability.AVAILABLE
            if self._config_snapshot.known
            else SnapshotAvailability.UNKNOWN
            if any(
                self._capabilities.supports(capability)
                for capability in (
                    AgentCapability.LIST_MODELS,
                    AgentCapability.SET_MODEL,
                    AgentCapability.READ_EFFORT,
                    AgentCapability.SET_EFFORT,
                )
            )
            else SnapshotAvailability.UNSUPPORTED
        )
        return AgentRuntimeProjection(
            capabilities=self._capabilities,
            session=AgentSessionSnapshot(
                SnapshotAvailability.AVAILABLE,
                session_ref=self.session_ref,
                status=(
                    "closed"
                    if self._closed
                    else "closing"
                    if self._closing
                    else "failed"
                    if self._failure_emitted
                    else "running"
                    if self._active_turn_ref is not None
                    else "idle"
                ),
                model=self._model,
                backend=self._backend,
                loaded=self._loaded,
                preview="",
                auth_owner="runtime",
                auth_method_ids=self._auth_method_ids,
            ),
            work=AgentWorkSnapshot(SnapshotAvailability.UNSUPPORTED),
            context=(
                AgentContextSnapshot(
                    SnapshotAvailability.AVAILABLE,
                    used_tokens=self._usage.used_tokens,
                    model_context_window=self._usage.context_window,
                )
                if self._capabilities.supports(AgentCapability.CONTEXT_USAGE)
                and self._usage is not None
                else AgentContextSnapshot(SnapshotAvailability.UNKNOWN)
                if self._capabilities.supports(AgentCapability.CONTEXT_USAGE)
                else AgentContextSnapshot(SnapshotAvailability.UNSUPPORTED)
            ),
            background=AgentBackgroundSnapshot(SnapshotAvailability.UNSUPPORTED),
            configuration=AgentConfigurationSnapshot(
                configuration_availability,
                models=(
                    model_option.values
                    if model_option is not None and model_option.option_type == "select"
                    else ()
                ),
                current_model=(
                    model_option.current_value
                    if model_option is not None
                    and isinstance(model_option.current_value, str)
                    else ""
                ),
                effort_values=(
                    effort_option.values
                    if effort_option is not None and effort_option.option_type == "select"
                    else ()
                ),
                current_effort=(
                    effort_option.current_value
                    if effort_option is not None
                    and isinstance(effort_option.current_value, str)
                    else ""
                ),
            ),
        )

    def poll_events(self) -> list[AgentSessionEvent]:
        self._host._drain()
        events = list(self._events)
        self._events.clear()
        return events

    def _receive_turn_output(self, turn_ref: TurnRef, text: str) -> None:
        if not self._is_active(turn_ref):
            return
        if self._output_text.tell() + len(text) > self._turn_output_max_chars:
            self._fail_closed(
                turn_ref,
                "ACP turn output limit exceeded "
                f"({self._turn_output_max_chars} characters)",
            )
            return
        if not self._queue_turn_event(OutputDelta(turn_ref, text), turn_ref):
            return
        self._output_text.write(text)

    def _receive_config_snapshot(self, snapshot: _ConfigSnapshot) -> None:
        if self._closed or self._failure_emitted:
            return
        self._apply_config_snapshot(snapshot)

    def _receive_usage(self, usage: _SessionUsage) -> None:
        if self._closed or self._failure_emitted:
            return
        if self._usage_blocked_after_cancel:
            if self._active_turn_ref is not None:
                self._pending_usage_after_cancel = usage
            return
        self._usage = usage

    def _receive_config_selection_accepted(
        self,
        category: str,
        value: str,
        snapshot: _ConfigSnapshot,
    ) -> None:
        if self._pending_config != (category, value) or self._closed:
            return
        self._pending_config = None
        self._apply_config_snapshot(snapshot)
        event: AgentSessionEvent = (
            ModelSelectionAccepted(value)
            if category == "model"
            else EffortSelectionAccepted(value)
        )
        self._queue_control_event(event)

    def _receive_config_selection_failed(
        self, category: str, value: str, error: str
    ) -> None:
        if self._pending_config != (category, value) or self._closed:
            return
        self._pending_config = None
        event: AgentSessionEvent = (
            ModelSelectionFailed(value, error)
            if category == "model"
            else EffortSelectionFailed(value, error)
        )
        self._queue_control_event(event)

    def _receive_config_selection_uncertain(
        self, category: str, value: str, error: str
    ) -> None:
        if self._pending_config != (category, value) or self._closed:
            return
        self._pending_config = None
        event: AgentSessionEvent = (
            ModelSelectionUncertain(value, error)
            if category == "model"
            else EffortSelectionUncertain(value, error)
        )
        if self._queue_control_event(event):
            self._fail_closed(None, error)

    def _receive_metadata_failure(self, error: str) -> None:
        self._fail_closed(self._active_turn_ref, error)

    def _apply_config_snapshot(self, snapshot: _ConfigSnapshot) -> None:
        previous_model = self._model
        effective = _effective_from_snapshot(
            snapshot,
            fallback_backend=self._host._config.runtime_profile.backend_name,
        )
        self._config_snapshot = snapshot
        self._model = effective.model
        self._backend = effective.backend
        if self._model != previous_model:
            self._usage = None

    def _queue_control_event(self, event: AgentSessionEvent) -> bool:
        if len(self._events) < self._event_capacity:
            self._events.append(event)
            return True
        self._fail_closed(
            self._active_turn_ref,
            "ACP session event backlog limit exceeded "
            f"({self._event_capacity} events)",
            replace_pending=True,
        )
        return False

    def _receive_approval_requested(
        self,
        turn_ref: TurnRef,
        request: AgentApprovalRequest,
    ) -> None:
        if not self._is_active(turn_ref):
            return
        if self._active_approval_request is not None or self._active_form_request is not None:
            self._fail_closed(turn_ref, "ACP callback responder ownership conflict")
            return
        self._active_approval_request = request
        self._queue_turn_event(
            ApprovalRequested(turn_ref, request.approval_ref, request),
            turn_ref,
        )

    def _receive_provider_notice(self, turn_ref: TurnRef, text: str) -> None:
        if not self._is_active(turn_ref):
            return
        self._queue_turn_event(ProviderNotice(text, turn_ref), turn_ref)

    def _receive_elicitation_requested(
        self,
        turn_ref: TurnRef,
        request: AgentFormRequest,
    ) -> None:
        if not self._is_active(turn_ref):
            return
        if self._active_approval_request is not None or self._active_form_request is not None:
            self._fail_closed(turn_ref, "ACP callback responder ownership conflict")
            return
        self._active_form_request = request
        self._queue_turn_event(
            ElicitationRequested(turn_ref, request.elicitation_ref, request),
            turn_ref,
        )

    def _receive_turn_completed(self, turn_ref: TurnRef) -> None:
        if not self._is_active(turn_ref):
            return
        if not self._queue_turn_event(
            TurnCompleted(turn_ref, AgentOutput(self._output_text.getvalue())),
            turn_ref,
        ):
            return
        if self._usage_blocked_after_cancel:
            self._usage = self._pending_usage_after_cancel
            self._pending_usage_after_cancel = None
        self._clear_turn(turn_ref)

    def _receive_turn_failed(
        self,
        turn_ref: TurnRef,
        error: str,
        *,
        raw_provider_error: RawProviderErrorPayload | None = None,
        fatal: bool,
    ) -> None:
        if not self._is_active(turn_ref):
            return
        if fatal:
            self._fail_closed(turn_ref, error)
            return
        if not self._queue_turn_event(
            TurnFailed(turn_ref, error, raw_provider_error),
            turn_ref,
        ):
            return
        if self._usage_blocked_after_cancel:
            self._pending_usage_after_cancel = None
        self._clear_turn(turn_ref)

    def _receive_turn_interrupted(self, turn_ref: TurnRef) -> None:
        if not self._is_active(turn_ref):
            return
        if not self._queue_turn_event(TurnInterrupted(turn_ref), turn_ref):
            return
        self._pending_usage_after_cancel = None
        self._clear_turn(turn_ref)

    def _receive_host_failure(self, error: str) -> None:
        if self._closed or self._failure_emitted:
            return
        pending = self._pending_config
        if pending is not None:
            category, value = pending
            event: AgentSessionEvent = (
                ModelSelectionUncertain(value, error)
                if category == "model"
                else EffortSelectionUncertain(value, error)
            )
            if len(self._events) < self._event_capacity:
                self._events.append(event)
        self._fail_closed(self._active_turn_ref, error)

    def _queue_turn_event(self, event: AgentSessionEvent, turn_ref: TurnRef) -> bool:
        if len(self._events) < self._event_capacity:
            self._events.append(event)
            return True
        self._fail_closed(
            turn_ref,
            "ACP session event backlog limit exceeded "
            f"({self._event_capacity} events)",
            replace_pending=True,
        )
        return False

    def _fail_closed(
        self,
        turn_ref: TurnRef | None,
        error: str,
        *,
        replace_pending: bool = False,
    ) -> None:
        if self._closed or self._failure_emitted:
            return
        terminal_events: list[AgentSessionEvent] = []
        if turn_ref is not None:
            terminal_events.append(TurnFailed(turn_ref, error))
        terminal_events.append(SessionFailed(error))
        if replace_pending or len(self._events) + len(terminal_events) > self._event_capacity:
            self._events.clear()
        self._events.extend(terminal_events)
        self._failure_emitted = True
        self._active_turn_ref = None
        self._interrupt_requested_ref = None
        self._active_approval_request = None
        self._active_form_request = None
        self._pending_config = None
        self._pending_usage_after_cancel = None
        self._output_text = StringIO()
        if turn_ref is not None:
            self._host._abandon_turn(self, turn_ref)

    def _is_active(self, turn_ref: TurnRef) -> bool:
        return (
            not self._closed
            and not self._failure_emitted
            and self._active_turn_ref == turn_ref
        )

    def _clear_turn(self, turn_ref: TurnRef) -> None:
        if self._active_turn_ref == turn_ref:
            self._active_turn_ref = None
            self._interrupt_requested_ref = None
            self._active_approval_request = None
            self._active_form_request = None
            self._output_text = StringIO()

    def _unsupported(self, effect: AgentSessionEffect) -> AgentEffectResult:
        result = unsupported_effect_result(effect, self._capabilities)
        if result is None:
            raise AssertionError(f"unexpectedly supported ACP effect: {type(effect).__name__}")
        return result


def acp_capability_snapshot(
    profile: AcpRuntimeProfileConfig,
    *,
    runtime_version: str = "unknown",
) -> AgentCapabilitySnapshot:
    return build_acp_capability_snapshot(
        adapter_name=ACP_ADAPTER_COMPONENT,
        adapter_version=ACP_ADAPTER_VERSION,
        runtime_name=profile.runtime_name,
        runtime_version=runtime_version,
        backend_name=profile.backend_name,
        backend_version=profile.backend_version,
        supported=profile.supported_capabilities,
    )


def register_acp_provider(
    registry: ProviderRegistry,
    *,
    profile: AcpRuntimeProfileConfig,
    context: ProviderRegistrationContext,
) -> None:
    registered_env = MappingProxyType(
        dict(
            context.runtime_environment_for(
                profile.provider,
                inherit_parent=profile.inherit_parent_environment,
            )
        )
    )
    cli_path = context.cli_path_for_profile(profile.execution_profile)

    def resolve_command(command: tuple[str, ...]) -> tuple[str, ...]:
        if cli_path is None:
            return command
        return (str(cli_path), *command[1:])

    runtime_command = resolve_command(profile.command)

    def compose(
        context: ProviderHostRootConfiguration,
        _initial_session_config: AgentSessionConfig,
    ) -> ComposedProviderHost:
        if context.runtime_command != runtime_command:
            raise RuntimeError("ACP shared host command differs from registration")
        config = AcpProviderConfig(
            runtime_profile=profile,
            logger=context.logger,
            command=context.runtime_command,
            env=registered_env,
            role=context.role,
            startup_timeout_sec=context.startup_timeout_sec,
            rpc_timeout_sec=context.rpc_timeout_sec,
            edge_thread_name=context.edge_thread_name.replace("jsonrpc", "acp-v1"),
        )
        return ComposedProviderHost(
            host=AcpProviderHost(config),
            initial_capabilities=acp_capability_snapshot(profile),
        )

    lifetime = (
        ProviderHostLifetime.SHARED_PROCESS
        if profile.host_lifetime == "shared-process"
        else ProviderHostLifetime.DEDICATED
    )
    registry.register(
        profile.execution_profile,
        AcpProviderConfig,
        AcpProviderHost,
        composer=compose,
        lifetime=lifetime,
        shared_runtime_command=(
            runtime_command
            if lifetime is ProviderHostLifetime.SHARED_PROCESS
            else None
        ),
    )
