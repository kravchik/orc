"""Codex provider host with dedicated process/session ownership."""

from __future__ import annotations

from dataclasses import dataclass, replace
from pathlib import Path
from types import MappingProxyType
from typing import Callable, Mapping

from orchestrator import clock
from orchestrator.adapters import (
    CodexJsonRpcSession,
    InteractiveSessionStartState,
    protocol_raw_capture_enabled,
)
from orchestrator.agent_identity import (
    ProviderExecutionProfile,
    ProviderKind,
    ProviderProfileId,
    ProviderSessionRef,
)
from orchestrator.agent_capabilities import codex_capability_snapshot
from orchestrator.agent_provider import (
    AgentSessionConfig,
    OpenSessionCorrelationId,
    OpenSessionError,
    OpenSessionErrorKind,
    OpenSessionOperation,
    OpenSessionRequest,
    EffectiveRuntimeSnapshot,
    ComposedProviderHost,
    ProviderDedicatedHostConfiguration,
    ProviderRegistrationContext,
    ProviderRegistry,
    ProviderStartupFailureCategory,
    ListProviderSessions,
    ProviderSessionCatalogEntry,
    ProviderSessionCatalogErrorKind,
    ProviderSessionsListFailed,
    ProviderSessionsListed,
    ResumeSessionRequest,
    OpenFailed,
    Opened,
    StartSessionRequest,
    StopHost,
    StopHostResult,
    classify_provider_startup_failure,
    publish_provider_startup_failure,
)
from orchestrator.agent_session import AgentSessionPort, CloseSession
from orchestrator.codex_agent_session import CodexAgentSessionAdapter
from orchestrator.codex_sessions import list_codex_cli_sessions
from orchestrator.jsonrpc_client_thread_driver import build_threaded_jsonrpc_client_factory
from orchestrator.processes import LifecycleLogger

@dataclass(frozen=True)
class CodexProviderConfig:
    command: tuple[str, ...]
    logger: LifecycleLogger
    rpc_timeout_sec: float
    rpc_retries: int
    role: str = "agent"
    startup_timeout_sec: float = 120.0
    env: Mapping[str, str] | None = None
    protocol_log_include_params: bool | None = None
    client_factory: Callable[..., object] | None = None
    edge_thread_name: str = "codex-jsonrpc-client-thread"
    transport_factory: Callable[..., CodexJsonRpcSession] = CodexJsonRpcSession
    initial_session_config: AgentSessionConfig | None = None

    def __post_init__(self) -> None:
        command = tuple(str(part) for part in self.command)
        if not command or any(not part for part in command):
            raise ValueError("Codex provider command must not be empty")
        if self.rpc_timeout_sec <= 0:
            raise ValueError("Codex RPC timeout must be positive")
        if self.rpc_retries < 0:
            raise ValueError("Codex RPC retries must not be negative")
        if self.startup_timeout_sec <= 0:
            raise ValueError("Codex startup timeout must be positive")
        if not isinstance(self.role, str) or not self.role.strip():
            raise ValueError("Codex provider role must be a non-empty string")
        if not isinstance(self.edge_thread_name, str) or not self.edge_thread_name.strip():
            raise ValueError("Codex provider edge thread name must be a non-empty string")
        if self.initial_session_config is not None and not isinstance(
            self.initial_session_config,
            AgentSessionConfig,
        ):
            raise ValueError("Codex initial session config must be AgentSessionConfig")
        object.__setattr__(self, "command", command)
        object.__setattr__(self, "role", self.role.strip())
        object.__setattr__(self, "edge_thread_name", self.edge_thread_name.strip())


class CodexProviderHost:
    """Owns one Codex app-server process and at most one logical session."""

    provider = ProviderKind.CODEX
    profile = ProviderProfileId.CODEX_NATIVE

    def __init__(self, config: CodexProviderConfig) -> None:
        self._config = config
        initial = config.initial_session_config
        self._transport = (
            self._new_transport(
                cwd=initial.cwd or None,
                thread_approval_policy=initial.approval_policy,
                sandbox=initial.sandbox,
            )
            if initial is not None
            else None
        )
        self._pending_open: _CodexOpenSessionOperation | None = None
        self._active_port: CodexAgentSessionAdapter | None = None
        self._opened_once = False
        self._stopped = False
        self._terminal_error_consumed = False
        self._last_session_config = initial or AgentSessionConfig()

    def open_session(self, request: OpenSessionRequest) -> OpenSessionOperation:
        if not isinstance(request, (StartSessionRequest, ResumeSessionRequest)):
            raise ValueError("Codex host requires a typed open session request")
        if self._stopped:
            return _FailedCodexOpenSessionOperation(
                request,
                OpenSessionError(OpenSessionErrorKind.CANCELLED, "Codex provider host is stopped"),
            )
        if isinstance(request, ResumeSessionRequest) and (
            request.session_ref.provider != self.provider
            or request.session_ref.profile != self.profile
        ):
            return _FailedCodexOpenSessionOperation(
                request,
                OpenSessionError(
                    OpenSessionErrorKind.INVALID_REQUEST,
                    "Codex host cannot resume a session owned by "
                    f"{request.session_ref.provider.value}/"
                    f"{request.session_ref.profile.value}",
                ),
            )
        if self._pending_open is not None and not self._pending_open.completed:
            return _FailedCodexOpenSessionOperation(
                request,
                OpenSessionError(OpenSessionErrorKind.BUSY, "Codex provider host is already opening a session"),
            )
        if self._opened_once:
            return _FailedCodexOpenSessionOperation(
                request,
                OpenSessionError(
                    OpenSessionErrorKind.BUSY,
                    "dedicated Codex provider host already opened a session",
                ),
            )
        try:
            transport = self._transport or self._new_transport_for_request(request)
        except Exception as exc:
            category = self._publish_startup_failure(exc)
            return _FailedCodexOpenSessionOperation(
                request,
                OpenSessionError(
                    OpenSessionErrorKind.STARTUP_FAILED,
                    _error_text(exc),
                    startup_failure_category=category,
                ),
            )
        return self._register_open(
            request=request,
            transport=transport,
            replacement_port=None,
        )

    def stop_host(self, effect: StopHost) -> StopHostResult:
        if not isinstance(effect, StopHost):
            raise ValueError("Codex provider host stop requires StopHost")
        if self._stopped:
            return StopHostResult(stopped=True)
        errors: list[str] = []
        pending = self._pending_open
        if pending is not None and not pending.completed:
            pending.cancel("Codex provider host stopped during session open")
        port = self._active_port
        if port is not None:
            result = port.close_session(CloseSession())
            if result.error:
                errors.append(result.error)
        try:
            if self._transport is not None:
                self._stop_transport(self._transport)
        except Exception as exc:
            errors.append(_error_text(exc))
        if errors:
            return StopHostResult(stopped=False, error="; ".join(errors))
        self._pending_open = None
        self._active_port = None
        self._transport = None
        self._stopped = True
        return StopHostResult(stopped=True)

    def consume_poll_progress(self) -> bool:
        transport = self._transport
        return transport.consume_transport_progress() if transport is not None else False

    def consume_terminal_error(self) -> str:
        if self._stopped or self._terminal_error_consumed:
            return ""
        transport = self._transport
        if transport is None:
            return ""
        returncode = transport.returncode
        if returncode is None:
            return ""
        self._terminal_error_consumed = True
        return f"app-server exited unexpectedly (returncode={returncode})"

    def _new_transport_for_request(self, request: OpenSessionRequest) -> CodexJsonRpcSession:
        return self._new_transport(
            cwd=request.config.cwd or None,
            thread_approval_policy=request.config.approval_policy,
            sandbox=request.config.sandbox,
        )

    def _new_transport(
        self,
        *,
        cwd: str | None,
        thread_approval_policy: str,
        sandbox: str,
    ) -> CodexJsonRpcSession:
        return self._config.transport_factory(
            role=self._config.role,
            command=list(self._config.command),
            logger=self._config.logger,
            cwd=cwd,
            env=self._config.env,
            startup_timeout_sec=self._config.startup_timeout_sec,
            rpc_timeout_sec=self._config.rpc_timeout_sec,
            rpc_retries=self._config.rpc_retries,
            thread_approval_policy=thread_approval_policy or "on-request",
            thread_sandbox=sandbox or "workspace-write",
            protocol_status_callback=None,
            protocol_log_include_params=(
                protocol_raw_capture_enabled()
                if self._config.protocol_log_include_params is None
                else self._config.protocol_log_include_params
            ),
            client_factory=build_threaded_jsonrpc_client_factory(
                base_factory=self._config.client_factory,
                thread_name=self._config.edge_thread_name,
            ),
        )

    def _register_open(
        self,
        *,
        request: OpenSessionRequest,
        transport: CodexJsonRpcSession,
        replacement_port: CodexAgentSessionAdapter | None,
    ) -> OpenSessionOperation:
        pending = self._pending_open
        if pending is not None and not pending.completed:
            raise RuntimeError("Codex provider host is already opening a session")
        operation = _CodexOpenSessionOperation(
            host=self,
            request=request,
            transport=transport,
            replacement_port=replacement_port,
        )
        self._last_session_config = request.config
        self._transport = transport
        self._pending_open = operation
        operation.dispatch()
        return operation

    def _publish_startup_failure(
        self,
        error: BaseException,
    ) -> ProviderStartupFailureCategory:
        category = classify_provider_startup_failure(error)
        publish_provider_startup_failure(
            self._config.logger,
            ProviderExecutionProfile(self.provider, self.profile),
            category,
        )
        return category

    def _stop_transport(self, transport: CodexJsonRpcSession) -> None:
        pid = transport.pid
        transport.stop()
        self._config.logger.event(
            "agent_backend_stopped",
            provider=self.provider.value,
            pid=pid,
            returncode=transport.returncode,
        )

    def _finish_open(
        self,
        operation: _CodexOpenSessionOperation,
        transport: CodexJsonRpcSession,
        replacement_port: CodexAgentSessionAdapter | None,
    ) -> Opened:
        if self._stopped:
            raise RuntimeError("Codex provider host stopped before session open completed")
        if self._pending_open is operation:
            self._pending_open = None
        self._transport = transport
        self._last_session_config = operation.request.config
        self._terminal_error_consumed = False
        port = replacement_port or self._new_port(transport)
        self._active_port = port
        self._opened_once = True
        session_ref = ProviderSessionRef(
            self.provider,
            str(transport.thread_id or ""),
            self.profile,
        )
        capabilities = transport.get_capability_snapshot()
        backend = str(transport.get_cached_thread_metadata().get("modelProvider") or "").strip()
        effective = EffectiveRuntimeSnapshot(
            model=str(transport.thread_model or "").strip(),
            backend=backend or capabilities.backend.name,
        )
        self._config.logger.event(
            "agent_backend_started",
            provider=self.provider.value,
            pid=transport.pid,
        )
        self._config.logger.event(
            "provider_session_opened",
            provider=self.provider.value,
            correlation_id=operation.correlation_id.value,
            provider_session_id=session_ref.native_id,
            effective_model=effective.model,
            effective_backend=effective.backend,
        )
        return Opened(
            correlation_id=operation.correlation_id,
            session=port,
            session_ref=session_ref,
            effective=effective,
            capabilities=capabilities,
        )

    def _open_failed(
        self,
        operation: _CodexOpenSessionOperation,
        error: OpenSessionError,
        *,
        release_transport: bool = True,
    ) -> OpenFailed:
        if self._pending_open is operation:
            self._pending_open = None
        if release_transport and self._transport is operation.transport:
            self._transport = None
        self._config.logger.event(
            "provider_session_open_failed",
            provider=self.provider.value,
            correlation_id=operation.correlation_id.value,
            error_kind=error.kind.value,
            error=error.message,
        )
        return OpenFailed(operation.correlation_id, error)

    def _new_port(self, transport: CodexJsonRpcSession) -> CodexAgentSessionAdapter:
        holder: list[CodexAgentSessionAdapter] = []
        port = CodexAgentSessionAdapter(
            session=transport,
            begin_model_change=self._begin_model_change,
            poll_model_change=self._poll_model_change,
            close_handler=lambda: self._close_session(holder[0]),
            consume_host_failure=self.consume_terminal_error,
            logger=self._config.logger,
            role=self._config.role,
        )
        holder.append(port)
        return port

    def _close_session(self, port: CodexAgentSessionAdapter) -> None:
        if self._active_port is port:
            self._active_port = None

    def _begin_model_change(self, model: str) -> object:
        port = self._active_port
        if port is None:
            raise RuntimeError("Codex provider session is not active")
        old_transport = self._transport
        if old_transport is None:
            raise RuntimeError("Codex provider transport is not available")
        config = self._session_config_with_model(model)
        typed_request = StartSessionRequest(OpenSessionCorrelationId.new(), config)
        try:
            self._stop_transport(old_transport)
        except Exception as exc:
            return _FailedCodexOpenSessionOperation(
                typed_request,
                OpenSessionError(OpenSessionErrorKind.STARTUP_FAILED, _error_text(exc)),
            )
        self._transport = None
        try:
            transport = self._new_transport_for_request(typed_request)
        except Exception as exc:
            return _FailedCodexOpenSessionOperation(
                typed_request,
                OpenSessionError(OpenSessionErrorKind.STARTUP_FAILED, _error_text(exc)),
            )
        return self._register_open(
            request=typed_request,
            transport=transport,
            replacement_port=port,
        )

    def _poll_model_change(
        self,
        state: object,
    ) -> tuple[CodexJsonRpcSession, Opened] | None:
        if not isinstance(state, OpenSessionOperation):
            raise RuntimeError("invalid Codex model change state")
        for event in state.poll_events():
            if isinstance(event, OpenFailed):
                raise RuntimeError(event.error.message)
            if isinstance(event, Opened):
                if not isinstance(state, _CodexOpenSessionOperation):
                    raise RuntimeError("Codex model change opened without a transport")
                return state.transport, event
        return None

    def _session_config_with_model(self, model: str) -> AgentSessionConfig:
        return replace(self._last_session_config, model=model)


class _CodexOpenSessionOperation(OpenSessionOperation):
    def __init__(
        self,
        *,
        host: CodexProviderHost,
        request: OpenSessionRequest,
        transport: CodexJsonRpcSession,
        replacement_port: CodexAgentSessionAdapter | None,
    ) -> None:
        super().__init__(request)
        self._host = host
        self.transport = transport
        self._replacement_port = replacement_port
        self._state: InteractiveSessionStartState | None = None
        self._started_at = clock.monotonic()
        self._last_phase = ""

    def dispatch(self) -> None:
        self._require_pending()
        try:
            resume_id = (
                self.request.session_ref.native_id
                if isinstance(self.request, ResumeSessionRequest)
                else None
            )
            start_params = {
                "approvalPolicy": self.request.config.approval_policy or "on-request",
                "sandbox": self.request.config.sandbox or "workspace-write",
            }
            if self.request.config.model:
                start_params["model"] = self.request.config.model
            self._state = self.transport.begin_interactive_start(
                resume_thread_id=resume_id,
                start_params=start_params,
            )
            self._progress(
                phase="request_dispatched",
                message="Codex initialize request dispatched",
            )
        except Exception as exc:
            self._fail_startup(exc)

    def cancel(self, message: str) -> None:
        if not self.completed:
            self._fail(OpenSessionErrorKind.CANCELLED, message)

    def _advance(self) -> None:
        state = self._state
        if state is None:
            self._fail(OpenSessionErrorKind.STARTUP_FAILED, "Codex session open was not dispatched")
            return
        returncode = self.transport.returncode
        if returncode is not None:
            self._fail(
                OpenSessionErrorKind.PROCESS_EXITED,
                f"app-server exited during startup (returncode={returncode})",
            )
            return
        if clock.monotonic() - self._started_at >= self._host._config.startup_timeout_sec:
            self._fail(
                OpenSessionErrorKind.STARTUP_TIMEOUT,
                f"startup timeout after {self._host._config.startup_timeout_sec:g}s",
            )
            return
        try:
            opened = self.transport.poll_interactive_start(state, timeout_sec=0.0)
        except Exception as exc:
            self._fail_startup(exc)
            return
        phase = _startup_phase(state)
        if phase and phase != self._last_phase:
            self._last_phase = phase
            self._progress(phase=phase, message=f"Codex startup phase: {phase}")
        if not opened:
            return
        try:
            result = self._host._finish_open(
                self,
                self.transport,
                self._replacement_port,
            )
        except Exception as exc:
            self._fail_startup(exc)
            return
        self._complete(result)

    def _fail_startup(self, error: BaseException) -> None:
        self._fail(
            OpenSessionErrorKind.STARTUP_FAILED,
            _error_text(error),
            startup_failure_category=self._host._publish_startup_failure(error),
        )

    def _progress(self, *, phase: str, message: str) -> None:
        self._host._config.logger.event(
            "provider_session_open_progress",
            provider=ProviderKind.CODEX.value,
            correlation_id=self.correlation_id.value,
            phase=phase,
            message=message,
        )
        self._publish_progress(phase=phase, message=message)

    def _fail(
        self,
        kind: OpenSessionErrorKind,
        message: str,
        *,
        startup_failure_category: ProviderStartupFailureCategory | None = None,
    ) -> None:
        if self.completed:
            return
        error = OpenSessionError(
            kind,
            message,
            startup_failure_category=startup_failure_category,
        )
        cleanup_succeeded = True
        try:
            self._host._stop_transport(self.transport)
        except Exception as exc:
            cleanup_succeeded = False
            error = OpenSessionError(
                kind,
                f"{message}; cleanup failed: {_error_text(exc)}",
                startup_failure_category=startup_failure_category,
            )
        self._complete(
            self._host._open_failed(
                self,
                error,
                release_transport=cleanup_succeeded,
            )
        )


class _FailedCodexOpenSessionOperation(OpenSessionOperation):
    def __init__(self, request: OpenSessionRequest, error: OpenSessionError) -> None:
        super().__init__(request)
        self._complete(OpenFailed(self.correlation_id, error))

    def _advance(self) -> None:
        return


def register_codex_provider(
    registry: ProviderRegistry,
    *,
    sessions_root: Path | None = None,
    env: Mapping[str, str] | None = None,
) -> None:
    registered_env = None if env is None else MappingProxyType(dict(env))

    def compose(
        context: ProviderDedicatedHostConfiguration,
        initial_session_config: AgentSessionConfig,
    ) -> ComposedProviderHost:
        config = CodexProviderConfig(
            command=context.runtime_command,
            logger=context.logger,
            rpc_timeout_sec=context.rpc_timeout_sec,
            rpc_retries=context.rpc_retries,
            role=context.role,
            startup_timeout_sec=context.startup_timeout_sec,
            env=registered_env,
            protocol_log_include_params=context.protocol_log_include_params,
            client_factory=context.edge_factory,
            edge_thread_name=context.edge_thread_name,
            initial_session_config=initial_session_config,
        )
        return ComposedProviderHost(
            host=CodexProviderHost(config),
            initial_capabilities=codex_capability_snapshot(),
        )

    def list_sessions(request: ListProviderSessions):
        try:
            sessions = list_codex_cli_sessions(
                project_cwd=request.cwd or None,
                sessions_root=sessions_root,
                limit=request.limit,
            )
        except (OSError, ValueError):
            return ProviderSessionsListFailed(
                ProviderSessionCatalogErrorKind.PROVIDER_FAILED,
                "Codex session catalog is unavailable",
            )
        return ProviderSessionsListed(
            tuple(
                ProviderSessionCatalogEntry(
                    session_ref=ProviderSessionRef(
                        ProviderKind.CODEX,
                        session.session_id,
                        ProviderProfileId.CODEX_NATIVE,
                    ),
                    name=session.thread_name,
                    cwd=session.cwd,
                    adapter_name="codex",
                    adapter_version="1",
                    updated_at=session.last_activity_iso,
                )
                for session in sessions
            )
        )

    registry.register(
        ProviderExecutionProfile(ProviderKind.CODEX, ProviderProfileId.CODEX_NATIVE),
        CodexProviderConfig,
        CodexProviderHost,
        composer=compose,
        session_catalog_loader=list_sessions,
    )


def register_provider(
    registry: ProviderRegistry,
    _context: ProviderRegistrationContext,
) -> None:
    """Common production registration entrypoint discovered by composition."""

    execution_profile = ProviderExecutionProfile(
        ProviderKind.CODEX,
        ProviderProfileId.CODEX_NATIVE,
    )
    register_codex_provider(
        registry,
        sessions_root=_context.session_catalog_root_for(execution_profile),
        env=_context.runtime_environment_for(ProviderKind.CODEX),
    )


def _startup_phase(state: InteractiveSessionStartState) -> str:
    if state.started:
        return "session_opened"
    if state.active_start_method == "thread/resume":
        return "session_resuming"
    if state.active_start_method == "thread/start":
        return "session_starting"
    return "transport_initializing"


def _error_text(exc: Exception) -> str:
    return str(exc).strip() or type(exc).__name__
