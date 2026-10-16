from __future__ import annotations

import json
from dataclasses import dataclass, field, replace
from pathlib import Path
from typing import Any, Callable, Protocol

from orchestrator.access_point_common import AccessPointKey
from orchestrator.agent_identity import (
    ProviderExecutionProfile,
    ProviderKind,
    ProviderProfileId,
    ProviderSessionRef,
)
from orchestrator.runtime_profile_catalog import (
    BUILTIN_RUNTIME_PROFILE_CATALOG,
    RuntimeProfileCatalog,
)
from orchestrator.approval_target import (
    HUMAN_APPROVAL_TARGET,
    sanitize_approval_targets,
)
from orchestrator.binding_address import ROUTING_MODE_SHAMAN, RoutingIdentityRegistry
from orchestrator.local_command_journal import LocalCommandJournal
from orchestrator.processes import LifecycleLogger
from orchestrator.safe_write import write_text_atomic
from orchestrator.uploads import (
    PendingUpload,
    pending_upload_from_payload,
    validate_loaded_pending,
)


RUNTIME_INTENT_BOUND_IDLE = "BOUND_IDLE"
RUNTIME_INTENT_RUNNING = "RUNNING"
LEGACY_REGISTRY_SCHEMA_VERSION = 1
PROVIDER_REGISTRY_SCHEMA_VERSION = 2
CURRENT_REGISTRY_SCHEMA_VERSION = 3


def normalize_runtime_intent(value: object) -> str:
    normalized = str(value or "").strip().upper()
    if normalized in {"RUNNING", "STARTING"}:
        return RUNTIME_INTENT_RUNNING
    return RUNTIME_INTENT_BOUND_IDLE


@dataclass
class PersistedAccessPointState:
    project_cwd: str
    agent: PersistedAgentBinding | None
    runtime_intent: str = RUNTIME_INTENT_BOUND_IDLE
    address: str = ""
    routing_mode: str = ""
    approval_target: str = HUMAN_APPROVAL_TARGET
    local_command_journal: LocalCommandJournal = field(default_factory=LocalCommandJournal)
    pending_uploads: tuple[PendingUpload, ...] = ()


@dataclass(frozen=True)
class ConfiguredProviderFields:
    cwd: str
    model: str = ""
    backend: str = ""
    mode: str = "proxy"
    approval_policy: str = ""
    sandbox: str = ""
    args: tuple[str, ...] = ()

    def to_payload(self) -> dict[str, Any]:
        return {
            "cwd": self.cwd,
            "model": self.model,
            "backend": self.backend,
            "mode": self.mode,
            "approval_policy": self.approval_policy,
            "sandbox": self.sandbox,
            "args": list(self.args),
        }


@dataclass(frozen=True)
class PersistedAgentBinding:
    agent_id: str
    provider: ProviderKind
    profile: ProviderProfileId
    session: ProviderSessionRef | None
    session_name: str
    configured: ConfiguredProviderFields
    adapter_name: str = ""
    adapter_version: str = ""
    runtime_name: str = ""
    runtime_version: str = ""

    def to_payload(self) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "agent_id": self.agent_id,
            "provider": self.provider.value,
            "profile": self.profile.value,
            "session_name": self.session_name,
            "configured": self.configured.to_payload(),
            "components": {
                "adapter": self.adapter_name,
                "adapter_version": self.adapter_version,
                "runtime": self.runtime_name,
                "runtime_version": self.runtime_version,
            },
        }
        if self.session is not None:
            payload["session"] = {
                "provider": self.session.provider.value,
                "profile": self.session.profile.value,
                "id": self.session.native_id,
            }
        return payload

    def to_runtime_spec(self) -> dict[str, Any]:
        spec: dict[str, Any] = {
            "provider": self.provider.value,
            "profile": self.profile.value,
            "cwd": self.configured.cwd,
            "model": self.configured.model,
            "backend": self.configured.backend,
            "mode": self.configured.mode,
            "approval_policy": self.configured.approval_policy,
            "sandbox": self.configured.sandbox,
            "args": list(self.configured.args),
        }
        if self.session is not None:
            spec["thread_id"] = self.session.native_id
        if self.session_name:
            spec["thread_name"] = self.session_name
        return spec


def parse_registry_schema_version(value: object) -> int:
    if value is None:
        return LEGACY_REGISTRY_SCHEMA_VERSION
    if (
        isinstance(value, bool)
        or not isinstance(value, int)
        or value not in {
            LEGACY_REGISTRY_SCHEMA_VERSION,
            PROVIDER_REGISTRY_SCHEMA_VERSION,
            CURRENT_REGISTRY_SCHEMA_VERSION,
        }
    ):
        raise ValueError(f"unsupported registry schema version: {value!r}")
    return value


def parse_persisted_agent_binding(
    raw: object,
    *,
    project_cwd: str,
    schema_version: int,
    profile_catalog: RuntimeProfileCatalog = BUILTIN_RUNTIME_PROFILE_CATALOG,
) -> PersistedAgentBinding | None:
    if not isinstance(raw, dict):
        return None
    resolved_version = parse_registry_schema_version(schema_version)
    if resolved_version in {
        PROVIDER_REGISTRY_SCHEMA_VERSION,
        CURRENT_REGISTRY_SCHEMA_VERSION,
    }:
        configured_raw = raw.get("configured")
        if not isinstance(configured_raw, dict):
            raise ValueError("agent.configured must be an object")
        provider = profile_catalog.require_provider(raw.get("provider"))
        if (
            resolved_version == PROVIDER_REGISTRY_SCHEMA_VERSION
            and provider != ProviderKind.CODEX
        ):
            raise ValueError(
                "schema version 2 supports implicit profile only for provider=codex"
            )
        profile = profile_catalog.resolve(
            provider,
            raw.get("profile")
            if resolved_version == CURRENT_REGISTRY_SCHEMA_VERSION
            else None,
        ).profile
        if resolved_version == CURRENT_REGISTRY_SCHEMA_VERSION and "profile" not in raw:
            raise ValueError("agent.profile is required")
        launch = profile_catalog.normalize_launch_configuration(
            profile_catalog.resolve(provider, profile),
            args=configured_raw.get("args"),
            provider_approval_policy=configured_raw.get("approval_policy"),
            sandbox=configured_raw.get("sandbox"),
        )
        configured = ConfiguredProviderFields(
            cwd=str(configured_raw.get("cwd") or project_cwd or ""),
            model=str(configured_raw.get("model") or "").strip(),
            backend=str(configured_raw.get("backend") or "").strip(),
            mode=str(configured_raw.get("mode") or "proxy").strip() or "proxy",
            approval_policy=launch.provider_approval_policy,
            sandbox=launch.sandbox,
            args=launch.args,
        )
        components_raw = raw.get("components")
        if components_raw is not None and not isinstance(components_raw, dict):
            raise ValueError("agent.components must be an object")
        components = components_raw if isinstance(components_raw, dict) else {}
        session_raw = raw.get("session")
        session: ProviderSessionRef | None = None
        if session_raw is not None:
            if not isinstance(session_raw, dict):
                raise ValueError("agent.session must be an object")
            session_provider = profile_catalog.require_provider(
                session_raw.get("provider")
            )
            if session_provider != provider:
                raise ValueError("agent.session provider must match agent provider")
            session_profile = profile_catalog.resolve(
                session_provider,
                session_raw.get("profile")
                if resolved_version == CURRENT_REGISTRY_SCHEMA_VERSION
                else profile,
            ).profile
            if (
                resolved_version == CURRENT_REGISTRY_SCHEMA_VERSION
                and "profile" not in session_raw
            ):
                raise ValueError("agent.session.profile is required")
            if session_profile != profile:
                raise ValueError("agent.session profile must match agent profile")
            session = ProviderSessionRef(
                session_provider,
                str(session_raw.get("id") or ""),
                session_profile,
            )
        return PersistedAgentBinding(
            agent_id=str(raw.get("agent_id") or "").strip(),
            provider=provider,
            profile=profile,
            session=session,
            session_name=str(raw.get("session_name") or "").strip(),
            configured=configured,
            adapter_name=str(components.get("adapter") or "").strip(),
            adapter_version=str(components.get("adapter_version") or "").strip(),
            runtime_name=str(components.get("runtime") or "").strip(),
            runtime_version=str(components.get("runtime_version") or "").strip(),
        )

    # Version 1 had no provider namespace and stored an open-ended runtime spec.
    spec = raw.get("spec") if isinstance(raw.get("spec"), dict) else {}
    provider = ProviderKind.CODEX
    profile = ProviderProfileId.CODEX_NATIVE
    native_session_id = str(raw.get("thread_id") or spec.get("thread_id") or "").strip()
    session = (
        ProviderSessionRef(provider, native_session_id, profile)
        if native_session_id
        else None
    )
    launch = profile_catalog.normalize_launch_configuration(
        ProviderExecutionProfile(provider, profile),
        args=spec.get("args"),
        provider_approval_policy=spec.get("approval_policy"),
        sandbox=spec.get("sandbox"),
    )
    return PersistedAgentBinding(
        agent_id=str(raw.get("agent_id") or "").strip(),
        provider=provider,
        profile=profile,
        session=session,
        session_name=str(raw.get("thread_name") or spec.get("thread_name") or "").strip(),
        configured=ConfiguredProviderFields(
            cwd=str(spec.get("cwd") or raw.get("cwd") or project_cwd or ""),
            model=str(spec.get("model") or raw.get("model") or "").strip(),
            backend=str(
                spec.get("backend")
                or raw.get("configured_backend")
                or raw.get("backend")
                or ""
            ).strip(),
            mode=str(spec.get("mode") or raw.get("mode") or "proxy").strip() or "proxy",
            approval_policy=launch.provider_approval_policy,
            sandbox=launch.sandbox,
            args=launch.args,
        ),
    )


def merge_runtime_persisted_state(
    previous: PersistedAccessPointState | None,
    runtime_state: PersistedAccessPointState,
) -> PersistedAccessPointState:
    if previous is None:
        return runtime_state
    return replace(
        runtime_state,
        local_command_journal=previous.local_command_journal,
        pending_uploads=previous.pending_uploads,
    )


def _is_access_point_id(value: object) -> bool:
    return isinstance(value, (int, str))


class AccessPointStateStore:
    """Persist Access Point -> runtime binding metadata for lazy restore."""

    def __init__(
        self,
        *,
        path: Path,
        logger: LifecycleLogger,
        profile_catalog: RuntimeProfileCatalog = BUILTIN_RUNTIME_PROFILE_CATALOG,
    ) -> None:
        self._path = path
        self._logger = logger
        self._profile_catalog = profile_catalog

    @property
    def path(self) -> Path:
        return self._path

    def load(self) -> dict[AccessPointKey, PersistedAccessPointState]:
        if not self._path.exists():
            self._logger.event(
                "registry_loaded",
                registry_path=str(self._path),
                restored_count=0,
                reason="missing_file",
            )
            return {}
        try:
            raw = self._path.read_text(encoding="utf-8")
            payload = json.loads(raw)
        except Exception as exc:
            self._logger.event(
                "registry_load_failed",
                registry_path=str(self._path),
                error=str(exc),
                error_type=type(exc).__name__,
            )
            return {}
        if not isinstance(payload, dict):
            self._logger.event(
                "registry_load_failed",
                registry_path=str(self._path),
                error="registry payload must be object",
                error_type="ValueError",
            )
            return {}
        try:
            schema_version = parse_registry_schema_version(payload.get("version"))
        except ValueError as exc:
            self._logger.event(
                "registry_load_failed",
                registry_path=str(self._path),
                error=str(exc),
                error_type=type(exc).__name__,
            )
            return {}
        entries = payload.get("access_points")
        if not isinstance(entries, list):
            self._logger.event(
                "registry_load_failed",
                registry_path=str(self._path),
                error="registry.access_points must be list",
                error_type="ValueError",
            )
            return {}
        restored: dict[AccessPointKey, PersistedAccessPointState] = {}
        for item in entries:
            if not isinstance(item, dict):
                continue
            ap = item.get("access_point")
            if not isinstance(ap, dict):
                continue
            ap_type = ap.get("type")
            chat_id = ap.get("chat_id")
            thread_id = ap.get("thread_id")
            if not isinstance(ap_type, str) or not _is_access_point_id(chat_id):
                continue
            if thread_id is not None and not _is_access_point_id(thread_id):
                continue
            project_cwd_raw = item.get("project_cwd")
            project_cwd = project_cwd_raw.strip() if isinstance(project_cwd_raw, str) else ""
            try:
                agent = parse_persisted_agent_binding(
                    item.get("agent"),
                    project_cwd=project_cwd,
                    schema_version=schema_version,
                    profile_catalog=self._profile_catalog,
                )
            except ValueError as exc:
                self._logger.event(
                    "agent_binding_restore_discarded",
                    access_point_type=ap_type,
                    chat_id=chat_id,
                    thread_id=thread_id,
                    error=str(exc),
                )
                agent = None
            runtime_intent = normalize_runtime_intent(item.get("runtime_intent"))
            address_raw = item.get("address")
            address = address_raw.strip() if isinstance(address_raw, str) else ""
            routing_mode_raw = item.get("routing_mode")
            routing_mode = (
                str(routing_mode_raw).strip().lower()
                if routing_mode_raw is not None
                else (ROUTING_MODE_SHAMAN if address else "")
            )
            approval_target_raw = item.get("approval_target")
            approval_target = (
                approval_target_raw.strip().lower()
                if isinstance(approval_target_raw, str) and approval_target_raw.strip()
                else HUMAN_APPROVAL_TARGET
            )
            local_command_journal = LocalCommandJournal.from_payload(
                item.get("local_command_journal")
            )
            pending_upload_list = [
                parsed
                for raw_upload in (
                    item.get("pending_uploads")
                    if isinstance(item.get("pending_uploads"), list)
                    else []
                )
                if (parsed := pending_upload_from_payload(raw_upload)) is not None
            ]
            try:
                validate_loaded_pending(pending_upload_list)
            except ValueError as exc:
                self._logger.event(
                    "pending_upload_restore_discarded",
                    access_point_type=ap_type,
                    chat_id=chat_id,
                    thread_id=thread_id,
                    error=str(exc),
                )
                pending_upload_list = []
            pending_uploads = tuple(pending_upload_list)
            access_point = AccessPointKey(
                type=ap_type.strip() or "telegram",
                chat_id=chat_id,
                thread_id=thread_id,
            )
            if local_command_journal.discarded_corrupted_entries > 0:
                self._logger.event(
                    "local_command_journal_restore_entries_discarded",
                    access_point_type=access_point.type,
                    access_point_chat_id=access_point.chat_id,
                    access_point_thread_id=access_point.thread_id,
                    discarded_count=local_command_journal.discarded_corrupted_entries,
                )
            restored[access_point] = PersistedAccessPointState(
                project_cwd=project_cwd,
                agent=agent,
                runtime_intent=runtime_intent,
                address=address,
                routing_mode=routing_mode,
                approval_target=approval_target,
                local_command_journal=local_command_journal,
                pending_uploads=pending_uploads,
            )
        try:
            if any(state.routing_mode and not state.address for state in restored.values()):
                raise ValueError("routing mode requires an address")
            RoutingIdentityRegistry(
                {
                    access_point: state.address
                    for access_point, state in restored.items()
                    if state.address
                },
                modes={
                    access_point: state.routing_mode
                    for access_point, state in restored.items()
                    if state.address
                },
            )
        except ValueError as exc:
            self._logger.event(
                "registry_load_failed",
                registry_path=str(self._path),
                error=str(exc),
                error_type=type(exc).__name__,
            )
            return {}
        addresses = {
            access_point: state.address
            for access_point, state in restored.items()
            if state.address
        }
        sanitized_targets, target_errors = sanitize_approval_targets(
            addresses=addresses,
            targets={
                access_point: state.approval_target
                for access_point, state in restored.items()
            },
        )
        for access_point, target in sanitized_targets.items():
            restored[access_point].approval_target = target
        for access_point, error in target_errors.items():
            self._logger.event(
                "approval_target_restore_fallback",
                access_point_type=access_point.type,
                chat_id=access_point.chat_id,
                thread_id=access_point.thread_id,
                error=error,
                fallback_target=HUMAN_APPROVAL_TARGET,
            )
        self._logger.event(
            "registry_loaded",
            registry_path=str(self._path),
            restored_count=len(restored),
            reason="ok",
        )
        return restored

    def save(
        self,
        records: dict[AccessPointKey, PersistedAccessPointState],
        *,
        sort_key: Callable[[AccessPointKey], tuple[str, str, str]],
    ) -> None:
        self._path.parent.mkdir(parents=True, exist_ok=True)
        access_points: list[dict[str, Any]] = []
        for access_point in sorted(records.keys(), key=sort_key):
            state = records[access_point]
            entry: dict[str, Any] = {
                "access_point": {
                    "type": access_point.type,
                    "chat_id": access_point.chat_id,
                    "thread_id": access_point.thread_id,
                },
                "project_cwd": state.project_cwd,
            }
            if state.agent is not None:
                entry["agent"] = state.agent.to_payload()
                entry["runtime_intent"] = normalize_runtime_intent(state.runtime_intent)
            if state.address:
                entry["address"] = state.address
                entry["routing_mode"] = state.routing_mode or ROUTING_MODE_SHAMAN
            if state.approval_target != HUMAN_APPROVAL_TARGET:
                entry["approval_target"] = state.approval_target
            if state.local_command_journal.has_state():
                entry["local_command_journal"] = state.local_command_journal.to_payload()
            if state.pending_uploads:
                entry["pending_uploads"] = [item.to_payload() for item in state.pending_uploads]
            access_points.append(entry)
        payload = {
            "version": CURRENT_REGISTRY_SCHEMA_VERSION,
            "access_points": access_points,
        }
        write_text_atomic(
            self._path,
            json.dumps(payload, ensure_ascii=True, indent=2) + "\n",
            encoding="utf-8",
        )
        self._logger.event(
            "registry_saved",
            registry_path=str(self._path),
            saved_count=len(records),
        )


class PersistedAgentRuntime(Protocol):
    def snapshot_persisted(self) -> dict[AccessPointKey, PersistedAccessPointState]: ...

    def restore_binding(
        self,
        access_point: AccessPointKey,
        *,
        binding: PersistedAgentBinding,
        runtime_intent: str = RUNTIME_INTENT_BOUND_IDLE,
        address: str = "",
        routing_mode: str = "",
        approval_target: str = HUMAN_APPROVAL_TARGET,
    ) -> dict[str, Any]: ...

@dataclass
class StewardRegistryContext:
    logger: LifecycleLogger
    agent_runtime: PersistedAgentRuntime
    state_store: AccessPointStateStore | None
    persisted_state_by_access_point: dict[AccessPointKey, PersistedAccessPointState]
    sort_key: Callable[[AccessPointKey], tuple[str, str, str]]

    def persist(self, *, reason: str, access_point: AccessPointKey | None = None) -> bool:
        if self.state_store is None:
            return True
        merged = dict(self.persisted_state_by_access_point)
        for key, runtime_state in self.agent_runtime.snapshot_persisted().items():
            merged[key] = merge_runtime_persisted_state(merged.get(key), runtime_state)
        pruned: dict[AccessPointKey, PersistedAccessPointState] = {}
        for key, value in merged.items():
            has_agent = value.agent is not None
            has_cwd = bool((value.project_cwd or "").strip())
            has_local_commands = value.local_command_journal.has_state()
            has_pending_uploads = bool(value.pending_uploads)
            if has_agent or has_cwd or has_local_commands or has_pending_uploads:
                pruned[key] = value
        try:
            self.state_store.save(pruned, sort_key=self.sort_key)
        except Exception as exc:
            self.logger.event(
                "registry_save_failed",
                registry_path=str(self.state_store.path),
                reason=reason,
                access_point_type=(access_point.type if access_point is not None else None),
                chat_id=(access_point.chat_id if access_point is not None else None),
                thread_id=(access_point.thread_id if access_point is not None else None),
                error=str(exc),
                error_type=type(exc).__name__,
            )
            return False
        self.persisted_state_by_access_point.clear()
        self.persisted_state_by_access_point.update(pruned)
        self.logger.event(
            "access_point_registry_persisted",
            reason=reason,
            access_point_type=(access_point.type if access_point is not None else None),
            chat_id=(access_point.chat_id if access_point is not None else None),
            thread_id=(access_point.thread_id if access_point is not None else None),
            persisted_count=len(pruned),
        )
        return True

    def drop(self, access_point: AccessPointKey) -> bool:
        previous = self.persisted_state_by_access_point.pop(access_point, None)
        if previous is not None and previous.local_command_journal.has_state():
            self.persisted_state_by_access_point[access_point] = PersistedAccessPointState(
                project_cwd="",
                agent=None,
                local_command_journal=previous.local_command_journal,
            )
        return self.persist(reason="access_point_reset", access_point=access_point)


def build_steward_registry_context(
    *,
    logger: LifecycleLogger,
    agent_runtime: PersistedAgentRuntime,
    state_path: str | Path | None,
    sort_key: Callable[[AccessPointKey], tuple[str, str, str]],
    profile_catalog: RuntimeProfileCatalog = BUILTIN_RUNTIME_PROFILE_CATALOG,
) -> StewardRegistryContext:
    state_store: AccessPointStateStore | None = None
    persisted_state_by_access_point: dict[AccessPointKey, PersistedAccessPointState] = {}
    if state_path is not None and str(state_path).strip():
        state_store = AccessPointStateStore(
            path=Path(str(state_path)).expanduser(),
            logger=logger,
            profile_catalog=profile_catalog,
        )
        persisted_state_by_access_point = state_store.load()
        for access_point, state in list(persisted_state_by_access_point.items()):
            agent_binding = state.agent
            if agent_binding is None:
                continue
            if not agent_binding.configured.cwd.strip():
                continue
            try:
                agent_runtime.restore_binding(
                    access_point,
                    binding=agent_binding,
                    runtime_intent=state.runtime_intent,
                    address=state.address,
                    routing_mode=state.routing_mode,
                    approval_target=state.approval_target,
                )
            except Exception as exc:
                logger.event(
                    "access_point_restore_failed",
                    access_point_type=access_point.type,
                    chat_id=access_point.chat_id,
                    thread_id=access_point.thread_id,
                    error=str(exc),
                    error_type=type(exc).__name__,
                )
    return StewardRegistryContext(
        logger=logger,
        agent_runtime=agent_runtime,
        state_store=state_store,
        persisted_state_by_access_point=persisted_state_by_access_point,
        sort_key=sort_key,
    )
