from __future__ import annotations

import threading
from dataclasses import dataclass
from typing import Any, Callable, Optional, Protocol

from orchestrator.approval import ApprovalDecisionProvider, ApprovalPolicy
from orchestrator.codex_single_agent_backend import CodexSingleAgentBackend
from orchestrator.hub import InMemoryHub, Node
from orchestrator.processes import LifecycleLogger
from orchestrator.protocol_status import format_item_status_summary, format_protocol_status
from orchestrator.telegram_agent import TelegramInteractiveCodexDriver


AccessPointId = int | str


@dataclass(frozen=True)
class AccessPointKey:
    type: str
    chat_id: AccessPointId
    thread_id: AccessPointId | None


@dataclass
class _PersistedAccessPointState:
    project_cwd: str
    agent: dict[str, Any] | None


class StewardBackend(Protocol):
    def ask(self, prompt: str, chat_id: AccessPointId | None = None) -> str: ...

    def close(self) -> None: ...

    def set_protocol_status_callback(
        self,
        callback: Optional[Callable[[str, dict], None]],
    ) -> None: ...

    def set_protocol_item_callback(
        self,
        callback: Optional[Callable[[str, str], None]],
    ) -> None: ...

    def get_item_status_snapshot(self) -> list[dict[str, str]]: ...
    def get_item_status_snapshot_for_turn(self, turn_id: str) -> list[dict[str, str]]: ...
    def get_last_item_apply_info(self) -> dict | None: ...


StewardBackendFactory = Callable[[AccessPointKey, LifecycleLogger], StewardBackend]
NodeFactory = Callable[[AccessPointKey], Node]
AgentBackendFactory = Callable[[AccessPointKey, dict[str, Any], LifecycleLogger], StewardBackend]
AgentNodeFactory = Callable[[AccessPointKey, dict[str, Any]], Node]
StewardDriverFactory = Callable[[AccessPointKey, LifecycleLogger], TelegramInteractiveCodexDriver]
RuntimeDriverFactory = Callable[[AccessPointKey, dict[str, Any], LifecycleLogger], TelegramInteractiveCodexDriver]


def access_point_sort_key(access_point: AccessPointKey) -> tuple[str, str, str]:
    thread_id = "main" if access_point.thread_id is None else str(access_point.thread_id)
    return (access_point.type, str(access_point.chat_id), thread_id)


class _BackendStewardNode:
    """Node wrapper around a backend implementation."""

    def __init__(
        self,
        backend: StewardBackend,
        access_point: AccessPointKey,
        steward_prompt: str | None = None,
    ) -> None:
        self._backend = backend
        self._access_point = access_point
        self._event_callback = None
        self._state = "created"
        self._steward_prompt = (steward_prompt or "").strip()
        self._steward_prompt_sent = False

    def set_event_callback(self, callback) -> None:
        self._event_callback = callback

    def start(self) -> None:
        start_fn = getattr(self._backend, "start", None)
        if callable(start_fn):
            start_fn()
        self._state = "running"
        if self._event_callback is not None:
            self._event_callback("node_started", {})

    def stop(self) -> None:
        close_fn = getattr(self._backend, "close", None)
        if callable(close_fn):
            close_fn()
        self._state = "stopped"
        if self._event_callback is not None:
            self._event_callback("node_stopped", {})

    def send(self, text: str, *, status_update=None, approval_notify=None) -> str:
        _ = approval_notify
        if self._state != "running":
            raise RuntimeError("steward backend node is not running")
        request_text = str(text)
        prepend_prompt = bool(self._steward_prompt and not self._steward_prompt_sent)
        if prepend_prompt:
            request_text = (
                "You are running under Steward control-plane contract.\n"
                "Follow these rules strictly:\n\n"
                f"{self._steward_prompt}\n\n"
                "---\n"
                "Human message:\n"
                f"{request_text}"
            )
        self._backend.set_protocol_status_callback(
            (lambda method, params: status_update(format_protocol_status(method=method, params=params)))
            if status_update is not None
            else None
        )
        if hasattr(self._backend, "set_protocol_item_callback"):
            self._backend.set_protocol_item_callback(
                (lambda _role, changed_item_id: status_update(
                    format_item_status_summary(
                        self._backend.get_item_status_snapshot(),
                        changed_item_id=changed_item_id,
                    )
                ))
                if status_update is not None
                else None
            )
        try:
            response = self._backend.ask(request_text, chat_id=self._access_point.chat_id)
            if prepend_prompt:
                self._steward_prompt_sent = True
            return response
        finally:
            if hasattr(self._backend, "set_protocol_item_callback"):
                self._backend.set_protocol_item_callback(None)
            self._backend.set_protocol_status_callback(None)

    def get_item_status_snapshot(self) -> list[dict[str, str]]:
        if hasattr(self._backend, "get_item_status_snapshot"):
            return list(self._backend.get_item_status_snapshot())
        return []

    def get_item_status_snapshot_for_turn(self, turn_id: str) -> list[dict[str, str]]:
        getter = getattr(self._backend, "get_item_status_snapshot_for_turn", None)
        if not callable(getter):
            return self.get_item_status_snapshot()
        try:
            raw = getter(turn_id)
        except Exception:
            return []
        if not isinstance(raw, list):
            return []
        return [dict(item) for item in raw if isinstance(item, dict)]

    def get_last_item_apply_info(self) -> dict | None:
        getter = getattr(self._backend, "get_last_item_apply_info", None)
        if not callable(getter):
            return None
        try:
            info = getter()
        except Exception:
            return None
        if not isinstance(info, dict):
            return None
        return dict(info)


class _BackendRuntimeAgentNode:
    """Node wrapper around started runtime agent backend."""

    def __init__(self, backend: StewardBackend, access_point: AccessPointKey) -> None:
        self._backend = backend
        self._access_point = access_point
        self._event_callback = None
        self._state = "created"
        self._thread_id = ""
        self._requested_thread_id = ""
        self._thread_lifecycle = "started"
        self._thread_model = ""

    def set_event_callback(self, callback) -> None:
        self._event_callback = callback

    def start(self) -> None:
        requested_thread_id = str(getattr(self._backend, "_resume_thread_id", "") or "").strip()
        self._requested_thread_id = requested_thread_id
        start_fn = getattr(self._backend, "start", None)
        if callable(start_fn):
            start_fn()
        backend_thread_id = getattr(self._backend, "_thread_id", None)
        if isinstance(backend_thread_id, str) and backend_thread_id.strip():
            self._thread_id = backend_thread_id.strip()
        get_actual_thread_model = getattr(self._backend, "get_actual_thread_model", None)
        if callable(get_actual_thread_model):
            raw_model = str(get_actual_thread_model() or "").strip()
            if raw_model:
                self._thread_model = raw_model
        self._thread_lifecycle = "resumed" if requested_thread_id else "started"
        if not self._thread_model:
            raise RuntimeError("runtime agent start did not provide actual model")
        self._state = "running"
        if self._event_callback is not None:
            self._event_callback("node_started", {})

    def stop(self) -> None:
        close_fn = getattr(self._backend, "close", None)
        if callable(close_fn):
            close_fn()
        self._state = "stopped"
        if self._event_callback is not None:
            self._event_callback("node_stopped", {})

    def send(self, text: str, *, status_update=None, approval_notify=None) -> str:
        _ = approval_notify
        if self._state != "running":
            raise RuntimeError("runtime agent node is not running")
        if hasattr(self._backend, "set_protocol_status_callback"):
            self._backend.set_protocol_status_callback(
                (lambda method, params: status_update(format_protocol_status(method=method, params=params)))
                if status_update is not None
                else None
            )
        if hasattr(self._backend, "set_protocol_item_callback"):
            self._backend.set_protocol_item_callback(
                (lambda _role, changed_item_id: status_update(
                    format_item_status_summary(
                        self._backend.get_item_status_snapshot(),
                        changed_item_id=changed_item_id,
                    )
                ))
                if status_update is not None
                else None
            )
        try:
            return self._backend.ask(str(text), chat_id=self._access_point.chat_id)
        finally:
            if hasattr(self._backend, "set_protocol_item_callback"):
                self._backend.set_protocol_item_callback(None)
            if hasattr(self._backend, "set_protocol_status_callback"):
                self._backend.set_protocol_status_callback(None)

    def get_thread_id(self) -> str:
        return self._thread_id

    def get_thread_lifecycle(self) -> str:
        return self._thread_lifecycle

    def get_requested_thread_id(self) -> str:
        return self._requested_thread_id

    def get_thread_model(self) -> str:
        return self._thread_model

    def get_item_status_snapshot(self) -> list[dict[str, str]]:
        if hasattr(self._backend, "get_item_status_snapshot"):
            return list(self._backend.get_item_status_snapshot())
        return []

    def get_item_status_snapshot_for_turn(self, turn_id: str) -> list[dict[str, str]]:
        getter = getattr(self._backend, "get_item_status_snapshot_for_turn", None)
        if not callable(getter):
            return self.get_item_status_snapshot()
        try:
            raw = getter(turn_id)
        except Exception:
            return []
        if not isinstance(raw, list):
            return []
        return [dict(item) for item in raw if isinstance(item, dict)]

    def get_last_item_apply_info(self) -> dict | None:
        getter = getattr(self._backend, "get_last_item_apply_info", None)
        if not callable(getter):
            return None
        try:
            info = getter()
        except Exception:
            return None
        if not isinstance(info, dict):
            return None
        return dict(info)


@dataclass
class _AccessPointAgentBinding:
    node_id: str
    cwd: str
    model: str
    mode: str
    spec: dict[str, Any]
    thread_id: str


class AccessPointStewardRuntime:
    """In-memory AccessPointKey -> Node runtime map."""

    def __init__(self, logger: LifecycleLogger, node_factory: NodeFactory) -> None:
        self._logger = logger
        self._hub = InMemoryHub()
        self._node_factory = node_factory
        self._lock = threading.RLock()
        self._node_by_access_point: dict[AccessPointKey, str] = {}

    def send(
        self,
        access_point: AccessPointKey,
        text: str,
        *,
        status_update: Callable[[str], None] | None = None,
        approval_notify: Callable[[str], None] | None = None,
    ) -> str:
        node_id = self._ensure_node(access_point)
        return self._hub.send(
            node_id,
            text,
            status_update=status_update,
            approval_notify=approval_notify,
        )

    def close(self) -> None:
        with self._lock:
            node_ids = list(self._node_by_access_point.values())
        for node_id in node_ids:
            self._hub.stop(node_id)

    def reset(self, access_point: AccessPointKey) -> bool:
        with self._lock:
            node_id = self._node_by_access_point.pop(access_point, None)
        if node_id is None:
            return False
        try:
            self._hub.stop(node_id)
        except RuntimeError:
            return False
        return True

    def show_running(self, access_point: AccessPointKey) -> list[dict[str, str]]:
        with self._lock:
            node_id = self._node_by_access_point.get(access_point)
        if node_id is None:
            return []
        try:
            state = self._hub.status(node_id).state
        except RuntimeError:
            return []
        return [{"agent_id": node_id, "state": state}]

    def get_item_status_snapshot(self, access_point: AccessPointKey) -> list[dict[str, str]]:
        with self._lock:
            node_id = self._node_by_access_point.get(access_point)
        if node_id is None:
            return []
        try:
            node = self._hub.get_node(node_id)
        except RuntimeError:
            return []
        getter = getattr(node, "get_item_status_snapshot", None)
        if not callable(getter):
            return []
        try:
            raw = getter()
        except Exception:
            return []
        if not isinstance(raw, list):
            return []
        return [dict(item) for item in raw if isinstance(item, dict)]

    def get_item_status_snapshot_for_turn(self, access_point: AccessPointKey, turn_id: str) -> list[dict[str, str]]:
        with self._lock:
            node_id = self._node_by_access_point.get(access_point)
        if node_id is None:
            return []
        try:
            node = self._hub.get_node(node_id)
        except RuntimeError:
            return []
        getter = getattr(node, "get_item_status_snapshot_for_turn", None)
        if not callable(getter):
            return self.get_item_status_snapshot(access_point)
        try:
            raw = getter(turn_id)
        except Exception:
            return []
        if not isinstance(raw, list):
            return []
        return [dict(item) for item in raw if isinstance(item, dict)]

    def get_last_item_apply_info(self, access_point: AccessPointKey) -> dict | None:
        with self._lock:
            node_id = self._node_by_access_point.get(access_point)
        if node_id is None:
            return None
        try:
            node = self._hub.get_node(node_id)
        except RuntimeError:
            return None
        getter = getattr(node, "get_last_item_apply_info", None)
        if not callable(getter):
            return None
        try:
            info = getter()
        except Exception:
            return None
        if not isinstance(info, dict):
            return None
        return dict(info)

    def _ensure_node(self, access_point: AccessPointKey) -> str:
        with self._lock:
            existing = self._node_by_access_point.get(access_point)
            if existing is not None:
                return existing
            node_id = (
                f"ap-{access_point.type}-"
                f"{access_point.chat_id}-"
                f"{access_point.thread_id if access_point.thread_id is not None else 'main'}"
            )
            self._hub.create(self._node_factory(access_point), node_id=node_id)
            self._hub.start(node_id)
            self._node_by_access_point[access_point] = node_id
        self._logger.event(
            "access_point_node_spawned",
            access_point_type=access_point.type,
            chat_id=access_point.chat_id,
            thread_id=access_point.thread_id,
            node_id=node_id,
        )
        return node_id


class AccessPointAgentRuntime:
    """In-memory AccessPointKey -> started runtime agent node map."""

    def __init__(self, logger: LifecycleLogger, node_factory: AgentNodeFactory) -> None:
        self._logger = logger
        self._hub = InMemoryHub()
        self._node_factory = node_factory
        self._lock = threading.RLock()
        self._id_seq = 0
        self._binding_by_access_point: dict[AccessPointKey, _AccessPointAgentBinding] = {}

    def _node_state(self, node_id: str) -> str | None:
        try:
            return self._hub.status(node_id).state
        except RuntimeError:
            return None

    def _normalize_spec(self, spec: dict[str, Any]) -> dict[str, Any]:
        resolved_spec = dict(spec)
        resolved_spec["cwd"] = str(resolved_spec.get("cwd") or "")
        resolved_spec["model"] = str(resolved_spec.get("model") or "")
        resolved_spec["mode"] = str(resolved_spec.get("mode") or "proxy")
        if "thread_id" in resolved_spec and resolved_spec.get("thread_id") is None:
            resolved_spec.pop("thread_id", None)
        return resolved_spec

    def start_agent(self, access_point: AccessPointKey, spec: dict[str, Any]) -> dict[str, Any]:
        resolved_spec = self._normalize_spec(spec)
        requested_thread_id = str(resolved_spec.get("thread_id") or "").strip()
        with self._lock:
            existing = self._binding_by_access_point.get(access_point)
            if existing is not None:
                state = self._node_state(existing.node_id)
                if state == "running":
                    raise RuntimeError("agent is already running for this access point")
                if state is not None:
                    self._hub.stop(existing.node_id)
                self._binding_by_access_point.pop(access_point, None)
            self._id_seq += 1
            node_id = (
                f"agent-{access_point.type}-"
                f"{access_point.chat_id}-"
                f"{access_point.thread_id if access_point.thread_id is not None else 'main'}-"
                f"{self._id_seq}"
            )
            node = self._node_factory(access_point, resolved_spec)
            self._hub.create(node, node_id=node_id)
            self._hub.start(node_id)
            runtime_thread_id = ""
            thread_lifecycle = "started"
            requested_thread_id_from_node = requested_thread_id
            get_thread_id = getattr(node, "get_thread_id", None)
            if callable(get_thread_id):
                raw_thread_id = get_thread_id()
                if isinstance(raw_thread_id, str) and raw_thread_id.strip():
                    runtime_thread_id = raw_thread_id.strip()
            get_thread_lifecycle = getattr(node, "get_thread_lifecycle", None)
            if callable(get_thread_lifecycle):
                raw_lifecycle = str(get_thread_lifecycle() or "").strip().lower()
                if raw_lifecycle in {"started", "resumed"}:
                    thread_lifecycle = raw_lifecycle
            get_requested_thread_id = getattr(node, "get_requested_thread_id", None)
            if callable(get_requested_thread_id):
                raw_requested = str(get_requested_thread_id() or "").strip()
                if raw_requested:
                    requested_thread_id_from_node = raw_requested
            actual_model = str(resolved_spec.get("model") or "").strip()
            get_thread_model = getattr(node, "get_thread_model", None)
            if callable(get_thread_model):
                raw_model = str(get_thread_model() or "").strip()
                if raw_model:
                    actual_model = raw_model
            if runtime_thread_id:
                resolved_spec["thread_id"] = runtime_thread_id
            resolved_spec["model"] = actual_model
            binding = _AccessPointAgentBinding(
                node_id=node_id,
                cwd=str(resolved_spec.get("cwd") or ""),
                model=str(resolved_spec.get("model") or ""),
                mode=str(resolved_spec.get("mode") or "proxy"),
                spec=resolved_spec,
                thread_id=str(runtime_thread_id or resolved_spec.get("thread_id") or ""),
            )
            self._binding_by_access_point[access_point] = binding
        lifecycle_event = "thread_resumed" if thread_lifecycle == "resumed" else "thread_started"
        self._logger.event(
            lifecycle_event,
            access_point_type=access_point.type,
            chat_id=access_point.chat_id,
            thread_id=access_point.thread_id,
            agent_id=node_id,
            runtime_thread_id=binding.thread_id,
            requested_thread_id=requested_thread_id_from_node or "",
            mode=binding.mode,
            model=binding.model,
            cwd=binding.cwd,
        )
        self._logger.event(
            "access_point_agent_started",
            access_point_type=access_point.type,
            chat_id=access_point.chat_id,
            thread_id=access_point.thread_id,
            node_id=node_id,
            cwd=binding.cwd,
            model=binding.model,
            mode=binding.mode,
        )
        return {
            "agent_id": node_id,
            "state": "running",
            "cwd": binding.cwd,
            "model": binding.model,
            "mode": binding.mode,
            "thread_id": binding.thread_id,
        }

    def is_bound(self, access_point: AccessPointKey) -> bool:
        with self._lock:
            return access_point in self._binding_by_access_point

    def has_binding(self, access_point: AccessPointKey) -> bool:
        with self._lock:
            return access_point in self._binding_by_access_point

    def runtime_state(self, access_point: AccessPointKey) -> str:
        with self._lock:
            binding = self._binding_by_access_point.get(access_point)
        if binding is None:
            return "UNBOUND"
        state = self._node_state(binding.node_id)
        if state is None:
            return "BOUND_IDLE"
        if state == "running":
            return "RUNNING"
        return "BOUND_IDLE"

    def get_binding_info(self, access_point: AccessPointKey) -> dict[str, str] | None:
        with self._lock:
            binding = self._binding_by_access_point.get(access_point)
        if binding is None:
            return None
        return {
            "agent_id": binding.node_id,
            "cwd": binding.cwd,
            "model": binding.model,
            "mode": binding.mode,
            "thread_id": binding.thread_id,
        }

    def stop_agent(self, access_point: AccessPointKey) -> bool:
        with self._lock:
            binding = self._binding_by_access_point.get(access_point)
        if binding is None:
            return False
        state = self._node_state(binding.node_id)
        if state is None or state == "stopped":
            return True
        self._hub.stop(binding.node_id)
        return True

    def start_bound_agent(self, access_point: AccessPointKey) -> dict[str, Any]:
        with self._lock:
            binding = self._binding_by_access_point.get(access_point)
        if binding is None:
            raise RuntimeError("no bound agent for this access point")
        if self.runtime_state(access_point) == "RUNNING":
            return {
                "agent_id": binding.node_id,
                "state": "running",
                "cwd": binding.cwd,
                "model": binding.model,
                "mode": binding.mode,
                "thread_id": binding.thread_id,
            }
        return self.start_agent(access_point, dict(binding.spec))

    def restore_binding(
        self,
        access_point: AccessPointKey,
        *,
        spec: dict[str, Any],
        agent_id: str = "",
        thread_id: str = "",
    ) -> dict[str, str]:
        resolved_spec = self._normalize_spec(spec)
        if thread_id.strip():
            resolved_spec["thread_id"] = thread_id.strip()
        with self._lock:
            existing = self._binding_by_access_point.get(access_point)
            if existing is not None:
                state = self._node_state(existing.node_id)
                if state is not None:
                    self._hub.stop(existing.node_id)
                self._binding_by_access_point.pop(access_point, None)
            restored_id = agent_id.strip() or (
                f"restored-agent-{access_point.type}-"
                f"{access_point.chat_id}-"
                f"{access_point.thread_id if access_point.thread_id is not None else 'main'}"
            )
            binding = _AccessPointAgentBinding(
                node_id=restored_id,
                cwd=str(resolved_spec.get("cwd") or ""),
                model=str(resolved_spec.get("model") or ""),
                mode=str(resolved_spec.get("mode") or "proxy"),
                spec=resolved_spec,
                thread_id=str(resolved_spec.get("thread_id") or ""),
            )
            self._binding_by_access_point[access_point] = binding
        self._logger.event(
            "access_point_agent_binding_restored",
            access_point_type=access_point.type,
            chat_id=access_point.chat_id,
            thread_id=access_point.thread_id,
            agent_id=binding.node_id,
            cwd=binding.cwd,
            model=binding.model,
            mode=binding.mode,
            restored_thread_id=binding.thread_id,
        )
        return {
            "agent_id": binding.node_id,
            "cwd": binding.cwd,
            "model": binding.model,
            "mode": binding.mode,
            "thread_id": binding.thread_id,
        }

    def snapshot_persisted(self) -> dict[AccessPointKey, _PersistedAccessPointState]:
        with self._lock:
            items = list(self._binding_by_access_point.items())
        snapshot: dict[AccessPointKey, _PersistedAccessPointState] = {}
        for access_point, binding in items:
            snapshot[access_point] = _PersistedAccessPointState(
                project_cwd=binding.cwd,
                agent={
                    "agent_id": binding.node_id,
                    "thread_id": binding.thread_id,
                    "cwd": binding.cwd,
                    "model": binding.model,
                    "mode": binding.mode,
                    "spec": dict(binding.spec),
                },
            )
        return snapshot

    def reset(self, access_point: AccessPointKey) -> bool:
        with self._lock:
            binding = self._binding_by_access_point.pop(access_point, None)
        if binding is None:
            return False
        state = self._node_state(binding.node_id)
        if state is None:
            return True
        try:
            self._hub.stop(binding.node_id)
        except RuntimeError:
            return False
        return True

    def send(
        self,
        access_point: AccessPointKey,
        text: str,
        *,
        status_update: Callable[[str], None] | None = None,
        approval_notify: Callable[[str], None] | None = None,
    ) -> str:
        with self._lock:
            binding = self._binding_by_access_point.get(access_point)
        if binding is None:
            raise RuntimeError("no running agent is bound to this access point")
        if self._node_state(binding.node_id) != "running":
            raise RuntimeError("no running agent is bound to this access point")
        return self._hub.send(
            binding.node_id,
            text,
            status_update=status_update,
            approval_notify=approval_notify,
        )

    def show_running(self, access_point: AccessPointKey) -> list[dict[str, str]]:
        with self._lock:
            binding = self._binding_by_access_point.get(access_point)
        if binding is None:
            return []
        state = self._node_state(binding.node_id)
        if state != "running":
            return []
        return [
            {
                "agent_id": binding.node_id,
                "state": state,
                "cwd": binding.cwd,
                "model": binding.model,
                "mode": binding.mode,
                "thread_id": binding.thread_id,
            }
        ]

    def get_item_status_snapshot(self, access_point: AccessPointKey) -> list[dict[str, str]]:
        with self._lock:
            binding = self._binding_by_access_point.get(access_point)
        if binding is None:
            return []
        try:
            node = self._hub.get_node(binding.node_id)
        except RuntimeError:
            return []
        getter = getattr(node, "get_item_status_snapshot", None)
        if not callable(getter):
            return []
        try:
            raw = getter()
        except Exception:
            return []
        if not isinstance(raw, list):
            return []
        return [dict(item) for item in raw if isinstance(item, dict)]

    def get_item_status_snapshot_for_turn(self, access_point: AccessPointKey, turn_id: str) -> list[dict[str, str]]:
        with self._lock:
            binding = self._binding_by_access_point.get(access_point)
        if binding is None:
            return []
        try:
            node = self._hub.get_node(binding.node_id)
        except RuntimeError:
            return []
        getter = getattr(node, "get_item_status_snapshot_for_turn", None)
        if not callable(getter):
            return self.get_item_status_snapshot(access_point)
        try:
            raw = getter(turn_id)
        except Exception:
            return []
        if not isinstance(raw, list):
            return []
        return [dict(item) for item in raw if isinstance(item, dict)]

    def get_last_item_apply_info(self, access_point: AccessPointKey) -> dict | None:
        with self._lock:
            binding = self._binding_by_access_point.get(access_point)
        if binding is None:
            return None
        try:
            node = self._hub.get_node(binding.node_id)
        except RuntimeError:
            return None
        getter = getattr(node, "get_last_item_apply_info", None)
        if not callable(getter):
            return None
        try:
            info = getter()
        except Exception:
            return None
        if not isinstance(info, dict):
            return None
        return dict(info)

    def close(self) -> None:
        with self._lock:
            bindings = list(self._binding_by_access_point.values())
            self._binding_by_access_point.clear()
        for binding in bindings:
            if self._node_state(binding.node_id) is None:
                continue
            try:
                self._hub.stop(binding.node_id)
            except RuntimeError:
                continue


def _default_backend_factory(
    *,
    agent_command: list[str],
    request_timeout_sec: float,
    rpc_timeout_sec: float,
    rpc_retries: int,
    approval_policy: ApprovalPolicy,
    approval_decision_provider: ApprovalDecisionProvider | None,
    thread_approval_policy: str,
    thread_sandbox: str,
    client_factory: Callable[..., Any] | None = None,
) -> StewardBackendFactory:
    def _factory(access_point: AccessPointKey, logger: LifecycleLogger) -> StewardBackend:
        _ = access_point
        return CodexSingleAgentBackend(
            command=list(agent_command),
            logger=logger,
            request_timeout_sec=request_timeout_sec,
            rpc_timeout_sec=rpc_timeout_sec,
            rpc_retries=rpc_retries,
            approval_policy=approval_policy,
            approval_decision_provider=approval_decision_provider,
            thread_approval_policy=thread_approval_policy,
            thread_sandbox=thread_sandbox,
            client_factory=client_factory,
        )

    return _factory


def _default_agent_backend_factory(
    *,
    agent_command: list[str],
    request_timeout_sec: float,
    rpc_timeout_sec: float,
    rpc_retries: int,
    approval_policy: ApprovalPolicy,
    approval_decision_provider: ApprovalDecisionProvider | None,
    default_thread_approval_policy: str,
    default_thread_sandbox: str,
    client_factory: Callable[..., Any] | None = None,
) -> AgentBackendFactory:
    def _has_model_reasoning_effort_override(command: list[str]) -> bool:
        for idx, token in enumerate(command):
            if token in ("-c", "--config"):
                if idx + 1 < len(command):
                    if str(command[idx + 1]).strip().startswith("model_reasoning_effort"):
                        return True
                continue
            if token.startswith("--config="):
                if token.split("=", 1)[1].strip().startswith("model_reasoning_effort"):
                    return True
        return False

    def _factory(access_point: AccessPointKey, spec: dict[str, Any], logger: LifecycleLogger) -> StewardBackend:
        _ = access_point
        command = list(agent_command)
        raw_args = spec.get("args") or []
        if isinstance(raw_args, list):
            command.extend(str(item) for item in raw_args)
        model = str(spec.get("model") or "").strip()
        if model == "gpt-5-codex" and not _has_model_reasoning_effort_override(command):
            command.extend(["-c", 'model_reasoning_effort="high"'])
            logger.event(
                "runtime_agent_command_amended",
                access_point_type=access_point.type,
                chat_id=access_point.chat_id,
                thread_id=access_point.thread_id,
                amendment='model_reasoning_effort="high"',
                model=model,
            )
        thread_approval_policy = str(spec.get("approval_policy") or default_thread_approval_policy)
        thread_sandbox = str(spec.get("sandbox") or default_thread_sandbox)
        return CodexSingleAgentBackend(
            command=command,
            logger=logger,
            request_timeout_sec=request_timeout_sec,
            rpc_timeout_sec=rpc_timeout_sec,
            rpc_retries=rpc_retries,
            approval_policy=approval_policy,
            approval_decision_provider=approval_decision_provider,
            thread_approval_policy=thread_approval_policy,
            thread_sandbox=thread_sandbox,
            resume_thread_id=(str(spec.get("thread_id") or "").strip() or None),
            working_dir=str(spec.get("cwd") or ""),
            thread_model=model,
            client_factory=client_factory,
        )

    return _factory
