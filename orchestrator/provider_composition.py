"""Production composition root for provider hosts and delivery drivers."""

from __future__ import annotations

from importlib import import_module
import os
from pathlib import Path
import threading
from types import MappingProxyType
from typing import Callable, Mapping

from orchestrator.agent_delivery_driver import AgentDeliveryDriver
from orchestrator.agent_identity import (
    ProviderExecutionProfile,
    ProviderKind,
    ProviderProfileId,
    ProviderSessionRef,
)
from orchestrator.agent_provider import (
    AgentSessionConfig,
    ComposedProviderHost,
    ListProviderSessions,
    ProviderDedicatedHostConfiguration,
    ProviderHostConfigurationError,
    ProviderHostConfigurationErrorKind,
    ProviderHostRootConfiguration,
    ProviderHostRootSettings,
    ProviderHostLifetime,
    ProviderRegistrationContext,
    ProviderRegistry,
    ProviderSessionCatalogErrorKind,
    ProviderSessionCatalogHost,
    ProviderSessionsListFailed,
    ProviderSessionsListed,
    ProviderSessionsListResult,
    StopHost,
)
from orchestrator.approval import ApprovalPolicy
from orchestrator.processes import (
    CleanupError,
    CleanupStep,
    LifecycleLogger,
    cleanup_owned_resources,
)
from orchestrator.runtime_profile_catalog import (
    ProviderSessionProjector,
    RuntimeProfileCatalog,
    load_runtime_profile_catalog_from_environment,
)
from orchestrator.provider_installation import (
    ProviderInstallationStatus,
    preflight_runtime_installation,
)


def resolve_production_session_catalog_roots(
    *,
    explicit_roots: Mapping[ProviderExecutionProfile, str | Path] | None,
    state_path: str | Path | None,
    codex_sessions_root: str | Path | None,
) -> dict[ProviderExecutionProfile, Path]:
    roots = {
        execution_profile: Path(path)
        for execution_profile, path in dict(explicit_roots or {}).items()
    }
    if state_path is not None:
        state_root = Path(state_path).expanduser().parent
        roots.setdefault(
            ProviderExecutionProfile(
                ProviderKind.OPENCODE,
                ProviderProfileId.OPENCODE_NATIVE,
            ),
            state_root / "provider-sessions" / ProviderProfileId.OPENCODE_NATIVE.value,
        )
    if codex_sessions_root is not None:
        roots[
            ProviderExecutionProfile(
                ProviderKind.CODEX,
                ProviderProfileId.CODEX_NATIVE,
            )
        ] = Path(codex_sessions_root)
    return roots


class ProductionProviderComposition:
    """Own provider registration and process-root shared provider hosts."""

    def __init__(
        self,
        *,
        provider_cli_paths: Mapping[ProviderKind, str | Path] | None = None,
        profile_cli_paths: Mapping[ProviderExecutionProfile, str | Path] | None = None,
        profile_runtime_commands: Mapping[
            ProviderExecutionProfile, tuple[str, ...] | list[str]
        ] | None = None,
        provider_environments: Mapping[ProviderKind, Mapping[str, str]] | None = None,
        session_catalog_roots: Mapping[
            ProviderExecutionProfile, str | Path
        ] | None = None,
        shared_host_cleanup_timeout_sec: float = 20.0,
        profile_catalog: RuntimeProfileCatalog | None = None,
        shared_host_settings: ProviderHostRootSettings | None = None,
    ) -> None:
        if profile_catalog is None:
            profile_catalog = load_runtime_profile_catalog_from_environment()
        if not isinstance(profile_catalog, RuntimeProfileCatalog):
            raise ValueError("provider composition requires RuntimeProfileCatalog")
        self._profile_catalog = profile_catalog
        runtime_commands: dict[ProviderExecutionProfile, tuple[str, ...]] = {}
        for execution_profile, command in dict(
            profile_runtime_commands or {}
        ).items():
            if not isinstance(command, (tuple, list)):
                raise ValueError("profile runtime command must be a list or tuple")
            runtime_commands[execution_profile] = tuple(command)
        for execution_profile, command in runtime_commands.items():
            self._profile_catalog.definition(execution_profile)
            if not command or any(
                not isinstance(part, str) or not part.strip() or "\x00" in part
                for part in command
            ):
                raise ValueError(
                    "profile runtime commands must contain non-empty strings"
                )
        self._profile_runtime_commands = MappingProxyType(runtime_commands)
        self._registry = ProviderRegistry(profile_catalog)
        self._registration_context = ProviderRegistrationContext(
            cli_paths={
                provider: Path(path)
                for provider, path in dict(provider_cli_paths or {}).items()
            },
            environments=dict(provider_environments or {}),
            ambient_environment=dict(os.environ),
            profile_cli_paths={
                profile: Path(path)
                for profile, path in dict(profile_cli_paths or {}).items()
            },
            session_catalog_roots={
                profile: Path(path)
                for profile, path in dict(session_catalog_roots or {}).items()
            },
            shared_host_cleanup_timeout_sec=shared_host_cleanup_timeout_sec,
        )
        self._shared_hosts: dict[ProviderExecutionProfile, ComposedProviderHost] = {}
        self._composition_lock = threading.RLock()
        self._registration_lock = threading.RLock()
        self._profile_locks = {
            execution_profile: threading.RLock()
            for execution_profile in self._profile_catalog.execution_profiles
        }
        self._shared_host_configurations: dict[
            ProviderExecutionProfile,
            ProviderHostRootConfiguration,
        ] = {}
        if shared_host_settings is not None and not isinstance(
            shared_host_settings,
            ProviderHostRootSettings,
        ):
            raise ValueError(
                "provider composition shared_host_settings must be "
                "ProviderHostRootSettings"
            )
        self._shared_host_settings = shared_host_settings
        self._accepting_drivers = True
        self._log_initial_installation_warnings()

    @property
    def session_projector(self) -> ProviderSessionProjector:
        return self._profile_catalog.session_projector

    def require_provider(
        self,
        provider: ProviderKind | ProviderExecutionProfile | str,
        profile: ProviderProfileId | str | None = None,
    ) -> ProviderExecutionProfile:
        execution_profile = self._resolve_execution_profile(provider, profile)
        self._ensure_provider_registered(execution_profile)
        self._registry.require_registered(execution_profile)
        return execution_profile

    def host_lifetime(
        self,
        provider: ProviderKind | ProviderExecutionProfile | str,
        profile: ProviderProfileId | str | None = None,
    ) -> ProviderHostLifetime:
        execution_profile = self.require_provider(provider, profile)
        return self._registry.host_lifetime(execution_profile)

    def list_sessions(
        self,
        request: ListProviderSessions,
    ) -> ProviderSessionsListResult:
        if not isinstance(request, ListProviderSessions):
            raise ValueError("provider composition session listing requires typed request")
        with self._composition_lock:
            accepting_drivers = self._accepting_drivers
        if not accepting_drivers:
            return ProviderSessionsListFailed(
                ProviderSessionCatalogErrorKind.UNAVAILABLE,
                "provider composition is closed",
            )
        self._ensure_provider_registered(request.execution_profile)
        if self._registry.has_session_catalog_loader(request.execution_profile):
            return self._registry.list_sessions(request)
        if self._registry.host_lifetime(request.execution_profile) is not ProviderHostLifetime.SHARED_PROCESS:
            return ProviderSessionsListFailed(
                ProviderSessionCatalogErrorKind.UNSUPPORTED,
                "provider session catalog is unsupported",
            )
        if self._shared_host_settings is None:
            return ProviderSessionsListFailed(
                ProviderSessionCatalogErrorKind.UNAVAILABLE,
                "provider session catalog requires root-owned host configuration",
            )
        with self._profile_locks[request.execution_profile]:
            with self._composition_lock:
                if not self._accepting_drivers:
                    return ProviderSessionsListFailed(
                        ProviderSessionCatalogErrorKind.UNAVAILABLE,
                        "provider composition is closed",
                    )
                composed = self._shared_hosts.get(request.execution_profile)
            if composed is None:
                try:
                    composed, _created = self._create_shared_host_locked(
                        request.execution_profile
                    )
                except RuntimeError as exc:
                    if str(exc) != "provider composition is closing":
                        raise
                    return ProviderSessionsListFailed(
                        ProviderSessionCatalogErrorKind.UNAVAILABLE,
                        "provider composition is closed",
                    )
        if not isinstance(composed.host, ProviderSessionCatalogHost):
            return ProviderSessionsListFailed(
                ProviderSessionCatalogErrorKind.UNSUPPORTED,
                "provider session catalog is unsupported",
            )
        return self._registry.validate_session_listing(
            request,
            composed.host.list_sessions(request),
        )

    def list_agent_sessions(
        self,
        request: ListProviderSessions,
    ) -> tuple[ProviderSessionsListResult, ProviderInstallationStatus]:
        """List for `/agents` without starting an unavailable installation."""

        if not isinstance(request, ListProviderSessions):
            raise ValueError("agent session listing requires typed request")
        status = self.installation_status(request.execution_profile)
        with self._composition_lock:
            accepting_drivers = self._accepting_drivers
        if not accepting_drivers:
            return (
                ProviderSessionsListFailed(
                    ProviderSessionCatalogErrorKind.UNAVAILABLE,
                    "provider composition is closed",
                ),
                status,
            )
        if status.available:
            return self.list_sessions(request), status
        self._ensure_provider_registered(request.execution_profile)
        if self._registry.has_session_catalog_loader(request.execution_profile):
            result = self._registry.list_sessions(request)
        else:
            result = ProviderSessionsListed(())
        return result, status

    def installation_status(
        self,
        execution_profile: ProviderExecutionProfile,
    ) -> ProviderInstallationStatus:
        contract = self._profile_catalog.installation_contract(execution_profile)
        command = self._configured_runtime_command(execution_profile)
        inherit_parent = True
        if self._profile_catalog.is_acp_profile(execution_profile):
            inherit_parent = self._profile_catalog.acp_profile_config(
                execution_profile
            ).inherit_parent_environment
        return preflight_runtime_installation(
            execution_profile=execution_profile,
            contract=contract,
            configured_command=command,
            environment=self._registration_context.runtime_environment_for(
                execution_profile.provider,
                inherit_parent=inherit_parent,
            ),
        )

    def _configured_runtime_command(
        self,
        execution_profile: ProviderExecutionProfile,
    ) -> tuple[str, ...]:
        explicit = self._profile_runtime_commands.get(execution_profile)
        if explicit is not None:
            return explicit
        contract = self._profile_catalog.installation_contract(execution_profile)
        cli_path = self._registration_context.cli_path_for_profile(execution_profile)
        if cli_path is not None:
            return (str(cli_path), *contract.command[1:])
        return contract.command

    def _log_initial_installation_warnings(self) -> None:
        settings = self._shared_host_settings
        if settings is None:
            return
        for execution_profile in self._profile_catalog.execution_profiles:
            status = self.installation_status(execution_profile)
            if status.available:
                continue
            settings.logger.event(
                "provider_installation_unavailable",
                provider=execution_profile.provider.value,
                profile=execution_profile.profile.value,
                reason=status.kind.value,
            )

    def _resolve_execution_profile(
        self,
        provider: ProviderKind | ProviderExecutionProfile | str,
        profile: ProviderProfileId | str | None,
    ) -> ProviderExecutionProfile:
        if isinstance(provider, ProviderExecutionProfile):
            if profile is not None:
                raise ValueError("profile must not be repeated for an execution profile")
            self._profile_catalog.definition(provider)
            return provider
        return self._profile_catalog.resolve(provider, profile)

    def _ensure_provider_registered(
        self,
        execution_profile: ProviderExecutionProfile,
    ) -> None:
        with self._profile_locks[execution_profile]:
            if self._registry.is_registered(execution_profile):
                return
            if self._profile_catalog.is_acp_profile(execution_profile):
                from orchestrator.acp_provider import register_acp_provider

                with self._registration_lock:
                    if not self._registry.is_registered(execution_profile):
                        register_acp_provider(
                            self._registry,
                            profile=self._profile_catalog.acp_profile_config(
                                execution_profile
                            ),
                            context=self._registration_context,
                        )
                return
            module_name = f"orchestrator.{execution_profile.provider.value}_provider"
            try:
                module = import_module(module_name)
            except ModuleNotFoundError as exc:
                if exc.name != module_name:
                    raise
                return
            register = getattr(module, "register_provider", None)
            if not callable(register):
                raise RuntimeError(
                    f"provider module {module_name} has no callable register_provider"
                )
            with self._registration_lock:
                if not self._registry.is_registered(execution_profile):
                    register(self._registry, self._registration_context)

    def create_delivery_driver(
        self,
        *,
        provider: ProviderKind = ProviderKind.CODEX,
        profile: ProviderProfileId | str | None = None,
        command: list[str] | None,
        runtime_args: object = None,
        logger: LifecycleLogger,
        rpc_timeout_sec: float | None,
        rpc_retries: int | None,
        approval_policy: ApprovalPolicy | None,
        thread_approval_policy: str,
        thread_sandbox: str,
        resume_session_id: str | None,
        working_dir: str | None = None,
        initial_model: str | None = None,
        initial_backend: str | None = None,
        client_factory: Callable[..., object] | None,
        edge_thread_name: str | None = None,
        startup_timeout_sec: float | None = None,
        provider_role: str | None = None,
        status_source: str = "agent",
        protocol_log_include_params: bool | None = None,
        drain_status_batch: bool = False,
    ) -> AgentDeliveryDriver:
        with self._composition_lock:
            if not self._accepting_drivers:
                raise RuntimeError("provider composition is closing")
        execution_profile = self.require_provider(provider, profile)
        launch = self._profile_catalog.normalize_launch_configuration(
            execution_profile,
            args=runtime_args,
            provider_approval_policy=thread_approval_policy,
            sandbox=thread_sandbox,
        )
        session_config = AgentSessionConfig(
            cwd=str(working_dir or ""),
            model=str(initial_model or ""),
            backend=str(initial_backend or ""),
            approval_policy=launch.provider_approval_policy,
            sandbox=launch.sandbox,
        )
        lifetime = self._registry.host_lifetime(execution_profile)
        composed = None
        created_shared_host = False
        if lifetime is ProviderHostLifetime.SHARED_PROCESS:
            self._reject_caller_owned_shared_host_configuration(
                execution_profile,
                command=command,
                runtime_args=launch.args,
                rpc_timeout_sec=rpc_timeout_sec,
                edge_thread_name=edge_thread_name,
                startup_timeout_sec=startup_timeout_sec,
                provider_role=provider_role,
            )
            with self._profile_locks[execution_profile]:
                composed, created_shared_host = self._create_shared_host_locked(
                    execution_profile
                )
        else:
            configured_command = self._configured_runtime_command(execution_profile)
            if command is not None and tuple(command) != configured_command:
                raise ProviderHostConfigurationError(
                    ProviderHostConfigurationErrorKind.CALLER_COMMAND_MISMATCH,
                    execution_profile,
                )
            command = list(configured_command)
            if rpc_timeout_sec is None:
                raise ValueError("dedicated provider requires RPC timeout")
            if rpc_retries is None:
                raise ValueError("dedicated provider requires RPC retries")
            settings = ProviderHostRootSettings(
                logger=logger,
                rpc_timeout_sec=rpc_timeout_sec,
                role=provider_role or "agent",
                startup_timeout_sec=(
                    120.0 if startup_timeout_sec is None else startup_timeout_sec
                ),
                edge_thread_name=edge_thread_name or "codex-jsonrpc-client-thread",
            )
            composed = self._registry.compose_host(
                execution_profile,
                ProviderDedicatedHostConfiguration(
                    settings,
                    tuple((*command, *launch.args)),
                    rpc_retries=rpc_retries,
                    edge_factory=client_factory,
                    protocol_log_include_params=protocol_log_include_params,
                ),
                session_config,
            )
        host = composed.host
        try:
            resume_session_ref = (
                ProviderSessionRef(
                    execution_profile.provider,
                    resume_session_id,
                    execution_profile.profile,
                )
                if resume_session_id
                else None
            )
            return AgentDeliveryDriver(
                provider_host=host,
                logger=logger,
                session_config=session_config,
                resume_session_ref=resume_session_ref,
                initial_capabilities=composed.initial_capabilities,
                status_source=status_source,
                approval_policy=approval_policy,
                drain_status_batch=drain_status_batch,
                host_lifetime=lifetime,
                session_close_timeout_sec=(
                    self._registration_context.shared_host_cleanup_timeout_sec
                ),
            )
        except Exception as construction_error:
            if lifetime is ProviderHostLifetime.SHARED_PROCESS:
                if created_shared_host:
                    try:
                        self._stop_shared_host(execution_profile)
                    except Exception as cleanup_error:
                        raise RuntimeError(
                            f"delivery driver construction failed: {construction_error}; "
                            f"shared host rollback failed: {cleanup_error}"
                        ) from construction_error
                raise

            def stop_host() -> None:
                result = host.stop_host(StopHost())
                if not result.stopped:
                    raise RuntimeError(result.error)

            try:
                cleanup_owned_resources(
                    [
                        CleanupStep(
                            owner=(
                                f"{execution_profile.provider.value}/"
                                f"{execution_profile.profile.value} provider host"
                            ),
                            cleanup=stop_host,
                        )
                    ]
                )
            except CleanupError as cleanup_error:
                raise RuntimeError(
                    f"delivery driver construction failed: {construction_error}; "
                    f"rollback failed: {cleanup_error}"
                ) from construction_error
            raise

    def close(self) -> None:
        """Stop root-owned shared hosts; retain failed owners for cleanup retry."""
        with self._composition_lock:
            self._accepting_drivers = False
            execution_profiles = tuple(self._shared_hosts)
        steps = [
            CleanupStep(
                owner=(
                    f"shared {execution_profile.provider.value}/"
                    f"{execution_profile.profile.value} provider host"
                ),
                cleanup=lambda execution_profile=execution_profile: self._stop_shared_host(
                    execution_profile
                ),
            )
            for execution_profile in execution_profiles
        ]
        cleanup_owned_resources(steps)

    def _stop_shared_host(self, execution_profile: ProviderExecutionProfile) -> None:
        with self._profile_locks[execution_profile]:
            with self._composition_lock:
                composed = self._shared_hosts.get(execution_profile)
            if composed is None:
                return
            result = composed.host.stop_host(StopHost())
            if not result.stopped:
                raise RuntimeError(result.error)
            with self._composition_lock:
                if self._shared_hosts.get(execution_profile) is composed:
                    self._shared_hosts.pop(execution_profile, None)

    def _create_shared_host_locked(
        self,
        execution_profile: ProviderExecutionProfile,
    ) -> tuple[ComposedProviderHost, bool]:
        """Create under the caller-held profile lock; publish under the root lock."""

        with self._composition_lock:
            if not self._accepting_drivers:
                raise RuntimeError("provider composition is closing")
            composed = self._shared_hosts.get(execution_profile)
        if composed is not None:
            return composed, False

        configuration = self._shared_host_configuration(execution_profile)
        candidate = self._registry.compose_host(
            execution_profile,
            configuration,
            AgentSessionConfig(),
        )
        with self._composition_lock:
            if self._accepting_drivers:
                self._shared_hosts[execution_profile] = candidate
                return candidate, True

        result = candidate.host.stop_host(StopHost())
        if not result.stopped:
            raise RuntimeError(
                "provider composition closed during shared host creation and "
                f"rollback failed: {result.error}"
            )
        raise RuntimeError("provider composition is closing")

    def _shared_host_configuration(
        self,
        execution_profile: ProviderExecutionProfile,
    ) -> ProviderHostRootConfiguration:
        with self._composition_lock:
            configuration = self._shared_host_configurations.get(execution_profile)
            if configuration is not None:
                return configuration
            settings = self._shared_host_settings
        if settings is None:
            raise ProviderHostConfigurationError(
                ProviderHostConfigurationErrorKind.MISSING_ROOT,
                execution_profile,
            )
        configuration = ProviderHostRootConfiguration(
            settings,
            self._registry.shared_runtime_command(execution_profile),
        )
        with self._composition_lock:
            return self._shared_host_configurations.setdefault(
                execution_profile,
                configuration,
            )

    def _reject_caller_owned_shared_host_configuration(
        self,
        execution_profile: ProviderExecutionProfile,
        *,
        command: list[str] | None,
        runtime_args: tuple[str, ...],
        rpc_timeout_sec: float | None,
        edge_thread_name: str | None,
        startup_timeout_sec: float | None,
        provider_role: str | None,
    ) -> None:
        configuration = self._shared_host_configuration(execution_profile)
        command_matches = command is None or (
            isinstance(command, list)
            and all(isinstance(part, str) and part for part in command)
            and tuple(command) == configuration.runtime_command
        )
        rpc_timeout_matches = rpc_timeout_sec is None or (
            not isinstance(rpc_timeout_sec, bool)
            and isinstance(rpc_timeout_sec, (int, float))
            and float(rpc_timeout_sec) == configuration.rpc_timeout_sec
        )
        edge_thread_matches = edge_thread_name is None or (
            isinstance(edge_thread_name, str)
            and edge_thread_name.strip() == configuration.edge_thread_name
        )
        startup_timeout_matches = startup_timeout_sec is None or (
            not isinstance(startup_timeout_sec, bool)
            and isinstance(startup_timeout_sec, (int, float))
            and float(startup_timeout_sec) == configuration.startup_timeout_sec
        )
        role_matches = provider_role is None or (
            isinstance(provider_role, str)
            and provider_role.strip() == configuration.role
        )
        incompatible = any(
            (
                not command_matches,
                bool(runtime_args),
                not rpc_timeout_matches,
                not edge_thread_matches,
                not startup_timeout_matches,
                not role_matches,
            )
        )
        if not incompatible:
            return
        settings = self._shared_host_settings
        if settings is not None:
            settings.logger.event(
                "provider_host_configuration_rejected",
                provider=execution_profile.provider.value,
                profile=execution_profile.profile.value,
                category=ProviderHostConfigurationErrorKind.CALLER_OVERRIDE.value,
            )
        raise ProviderHostConfigurationError(
            ProviderHostConfigurationErrorKind.CALLER_OVERRIDE,
            execution_profile,
        )
