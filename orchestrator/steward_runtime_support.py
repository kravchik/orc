from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
import time
from typing import Any, Callable, Protocol

from orchestrator.approval import ApprovalPolicy
from orchestrator import clock
from orchestrator.processes import LifecycleLogger
from orchestrator.safe_write import write_text_atomic
from orchestrator.telegram_agent import (
    TelegramInteractiveCodexDriver,
    _TelegramAgentRequest,
)
from orchestrator.telegram_steward_helpers import (
    AccessPointKey,
    RuntimeDriverFactory,
    StewardDriverFactory,
    _PersistedAccessPointState,
)


def _is_access_point_id(value: object) -> bool:
    return isinstance(value, (int, str))


class AccessPointStateStore:
    """Persist Access Point -> runtime binding metadata for lazy restore."""

    def __init__(self, *, path: Path, logger: LifecycleLogger) -> None:
        self._path = path
        self._logger = logger

    @property
    def path(self) -> Path:
        return self._path

    def load(self) -> dict[AccessPointKey, _PersistedAccessPointState]:
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
        entries = payload.get("access_points")
        if not isinstance(entries, list):
            self._logger.event(
                "registry_load_failed",
                registry_path=str(self._path),
                error="registry.access_points must be list",
                error_type="ValueError",
            )
            return {}
        restored: dict[AccessPointKey, _PersistedAccessPointState] = {}
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
            agent_raw = item.get("agent")
            agent = dict(agent_raw) if isinstance(agent_raw, dict) else None
            restored[
                AccessPointKey(
                    type=ap_type.strip() or "telegram",
                    chat_id=chat_id,
                    thread_id=thread_id,
                )
            ] = _PersistedAccessPointState(project_cwd=project_cwd, agent=agent)
        self._logger.event(
            "registry_loaded",
            registry_path=str(self._path),
            restored_count=len(restored),
            reason="ok",
        )
        return restored

    def save(
        self,
        records: dict[AccessPointKey, _PersistedAccessPointState],
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
                entry["agent"] = dict(state.agent)
            access_points.append(entry)
        payload = {
            "version": 1,
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


@dataclass
class StewardDriverEvent:
    access_point: AccessPointKey
    source: str
    event: Any


@dataclass
class PendingStewardOperation:
    access_point: AccessPointKey
    source: str
    output_kind: Any
    phase: str = "initial"
    fallback_reply: str = ""


@dataclass
class _InteractiveStewardHandle:
    node_id: str
    driver: TelegramInteractiveCodexDriver
    steward_prompt_sent: bool = False


class StewardDeliveryLane(Protocol):
    def submit_request(
        self,
        access_point: AccessPointKey,
        text: str,
        *,
        context_note: str | None = None,
    ) -> None: ...
    def poll_once(self) -> list[StewardDriverEvent]: ...
    def submit_approval_decision(self, access_point: AccessPointKey, decision: str) -> None: ...
    def show_running(self, access_point: AccessPointKey) -> list[dict[str, str]]: ...
    def get_item_status_snapshot(self, access_point: AccessPointKey) -> list[dict[str, str]]: ...
    def get_item_status_snapshot_for_turn(self, access_point: AccessPointKey, turn_id: str) -> list[dict[str, str]]: ...
    def get_last_item_apply_info(self, access_point: AccessPointKey) -> dict | None: ...
    def reset(self, access_point: AccessPointKey) -> bool: ...
    def close(self) -> None: ...


class AgentDeliveryLane(StewardDeliveryLane, Protocol):
    def start_agent(self, access_point: AccessPointKey, spec: dict[str, Any]) -> dict[str, Any]: ...
    def is_bound(self, access_point: AccessPointKey) -> bool: ...
    def has_binding(self, access_point: AccessPointKey) -> bool: ...
    def runtime_state(self, access_point: AccessPointKey) -> str: ...
    def get_binding_info(self, access_point: AccessPointKey) -> dict[str, str] | None: ...
    def stop_agent(self, access_point: AccessPointKey) -> bool: ...
    def start_bound_agent(self, access_point: AccessPointKey) -> dict[str, Any]: ...
    def restore_binding(
        self,
        access_point: AccessPointKey,
        *,
        spec: dict[str, Any],
        agent_id: str = "",
        thread_id: str = "",
    ) -> dict[str, str]: ...
    def snapshot_persisted(self) -> dict[AccessPointKey, _PersistedAccessPointState]: ...


class InteractiveStewardRuntime:
    def __init__(
        self,
        *,
        logger: LifecycleLogger,
        agent_command: list[str],
        request_timeout_sec: float,
        rpc_timeout_sec: float,
        rpc_retries: int,
        approval_policy: ApprovalPolicy,
        thread_approval_policy: str,
        thread_sandbox: str,
        steward_prompt: str | None,
        client_factory: Callable[..., Any] | None,
        driver_factory: StewardDriverFactory | None = None,
        edge_thread_name: str = "telegram-steward-jsonrpc-client-thread",
    ) -> None:
        self._logger = logger
        self._agent_command = list(agent_command)
        self._request_timeout_sec = request_timeout_sec
        self._rpc_timeout_sec = rpc_timeout_sec
        self._rpc_retries = rpc_retries
        self._approval_policy = approval_policy
        self._thread_approval_policy = thread_approval_policy
        self._thread_sandbox = thread_sandbox
        self._steward_prompt = (steward_prompt or "").strip()
        self._client_factory = client_factory
        self._driver_factory = driver_factory
        self._edge_thread_name = edge_thread_name
        self._handles: dict[AccessPointKey, _InteractiveStewardHandle] = {}

    def submit_request(
        self,
        access_point: AccessPointKey,
        text: str,
        *,
        context_note: str | None = None,
    ) -> None:
        handle = self._ensure_handle(access_point)
        request_text = str(text)
        prepend_prompt = bool(self._steward_prompt and not handle.steward_prompt_sent)
        if prepend_prompt:
            notice_block = ""
            if context_note:
                notice_block = (
                    "System notice shown to user just before this message:\n"
                    f"{str(context_note).strip()}\n\n"
                )
            request_text = (
                "You are running under Steward control-plane contract.\n"
                "Follow these rules strictly:\n\n"
                f"{self._steward_prompt}\n\n"
                "---\n"
                f"{notice_block}"
                "Human message:\n"
                f"{request_text}"
            )
        self._logger.event(
            "steward_runtime_submit_request",
            access_point_type=access_point.type,
            chat_id=access_point.chat_id,
            thread_id=access_point.thread_id,
            node_id=handle.node_id,
            prepend_prompt=prepend_prompt,
            context_note=bool(context_note),
            text_preview=str(text).strip()[:200],
        )
        handle.driver.submit_request(
            _TelegramAgentRequest(
                chat_id=access_point.chat_id,
                thread_id=access_point.thread_id,
                prompt=request_text,
            )
        )
        if prepend_prompt:
            handle.steward_prompt_sent = True

    def poll_once(self) -> list[StewardDriverEvent]:
        out: list[StewardDriverEvent] = []
        for access_point, handle in list(self._handles.items()):
            for event in handle.driver.poll_once():
                self._logger.event(
                    "steward_runtime_driver_event",
                    access_point_type=access_point.type,
                    chat_id=access_point.chat_id,
                    thread_id=access_point.thread_id,
                    node_id=handle.node_id,
                    event_type=type(event).__name__,
                )
                out.append(StewardDriverEvent(access_point=access_point, source="steward", event=event))
        return out

    def show_running(self, access_point: AccessPointKey) -> list[dict[str, str]]:
        handle = self._handles.get(access_point)
        if handle is None:
            return []
        return [{"agent_id": handle.node_id, "state": "running"}]

    def get_item_status_snapshot(self, access_point: AccessPointKey) -> list[dict[str, str]]:
        handle = self._handles.get(access_point)
        if handle is None:
            return []
        return handle.driver.get_item_status_snapshot()

    def get_item_status_snapshot_for_turn(self, access_point: AccessPointKey, turn_id: str) -> list[dict[str, str]]:
        handle = self._handles.get(access_point)
        if handle is None:
            return []
        return handle.driver.get_item_status_snapshot_for_turn(turn_id)

    def get_last_item_apply_info(self, access_point: AccessPointKey) -> dict | None:
        handle = self._handles.get(access_point)
        if handle is None:
            return None
        return handle.driver.get_last_item_apply_info()

    def submit_approval_decision(self, access_point: AccessPointKey, decision: str) -> None:
        handle = self._handles.get(access_point)
        if handle is None:
            raise RuntimeError("no running steward node for this access point")
        self._logger.event(
            "steward_runtime_submit_approval_decision",
            access_point_type=access_point.type,
            chat_id=access_point.chat_id,
            thread_id=access_point.thread_id,
            node_id=handle.node_id,
            decision=decision,
        )
        handle.driver.submit_approval_decision(decision)

    def reset(self, access_point: AccessPointKey) -> bool:
        handle = self._handles.pop(access_point, None)
        if handle is None:
            return False
        handle.driver.stop()
        return True

    def close(self) -> None:
        handles = list(self._handles.values())
        self._handles.clear()
        for handle in handles:
            handle.driver.stop()

    def _ensure_handle(self, access_point: AccessPointKey) -> _InteractiveStewardHandle:
        existing = self._handles.get(access_point)
        if existing is not None:
            return existing
        node_id = (
            f"ap-{access_point.type}-"
            f"{access_point.chat_id}-"
            f"{access_point.thread_id if access_point.thread_id is not None else 'main'}"
        )
        driver = (
            self._driver_factory(access_point, self._logger)
            if self._driver_factory is not None
            else TelegramInteractiveCodexDriver(
                command=list(self._agent_command),
                logger=self._logger,
                request_timeout_sec=self._request_timeout_sec,
                rpc_timeout_sec=self._rpc_timeout_sec,
                rpc_retries=self._rpc_retries,
                approval_policy=self._approval_policy,
                thread_approval_policy=self._thread_approval_policy,
                thread_sandbox=self._thread_sandbox,
                resume_thread_id=None,
                working_dir=None,
                initial_model=None,
                client_factory=self._client_factory,
                edge_thread_name=self._edge_thread_name,
            )
        )
        driver.start()
        handle = _InteractiveStewardHandle(node_id=node_id, driver=driver)
        self._handles[access_point] = handle
        self._logger.event(
            "access_point_node_spawned",
            access_point_type=access_point.type,
            chat_id=access_point.chat_id,
            thread_id=access_point.thread_id,
            node_id=node_id,
        )
        return handle


@dataclass
class _InteractiveAgentBinding:
    node_id: str
    cwd: str
    model: str
    mode: str
    spec: dict[str, Any]
    thread_id: str
    requested_thread_id: str
    driver: TelegramInteractiveCodexDriver | None
    startup_logged: bool = False


class InteractiveAgentRuntime:
    def __init__(
        self,
        *,
        logger: LifecycleLogger,
        agent_command: list[str],
        request_timeout_sec: float,
        rpc_timeout_sec: float,
        rpc_retries: int,
        approval_policy: ApprovalPolicy,
        default_thread_approval_policy: str,
        default_thread_sandbox: str,
        client_factory: Callable[..., Any] | None,
        driver_factory: RuntimeDriverFactory | None = None,
        edge_thread_name: str = "telegram-runtime-jsonrpc-client-thread",
    ) -> None:
        self._logger = logger
        self._agent_command = list(agent_command)
        self._request_timeout_sec = request_timeout_sec
        self._rpc_timeout_sec = rpc_timeout_sec
        self._rpc_retries = rpc_retries
        self._approval_policy = approval_policy
        self._default_thread_approval_policy = default_thread_approval_policy
        self._default_thread_sandbox = default_thread_sandbox
        self._client_factory = client_factory
        self._driver_factory = driver_factory
        self._edge_thread_name = edge_thread_name
        self._id_seq = 0
        self._binding_by_access_point: dict[AccessPointKey, _InteractiveAgentBinding] = {}

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
        existing = self._binding_by_access_point.get(access_point)
        if existing is not None and existing.driver is not None:
            raise RuntimeError("agent is already running for this access point")
        self._id_seq += 1
        node_id = (
            f"agent-{access_point.type}-"
            f"{access_point.chat_id}-"
            f"{access_point.thread_id if access_point.thread_id is not None else 'main'}-"
            f"{self._id_seq}"
        )
        binding = _InteractiveAgentBinding(
            node_id=node_id,
            cwd=str(resolved_spec.get("cwd") or ""),
            model=str(resolved_spec.get("model") or ""),
            mode=str(resolved_spec.get("mode") or "proxy"),
            spec=resolved_spec,
            thread_id=str(resolved_spec.get("thread_id") or ""),
            requested_thread_id=str(resolved_spec.get("thread_id") or ""),
            driver=self._build_driver(access_point=access_point, spec=resolved_spec),
        )
        binding.driver.start()
        self._binding_by_access_point[access_point] = binding
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
            "agent_id": binding.node_id,
            "state": "running",
            "cwd": binding.cwd,
            "model": binding.model,
            "mode": binding.mode,
            "thread_id": binding.thread_id,
        }

    def is_bound(self, access_point: AccessPointKey) -> bool:
        return access_point in self._binding_by_access_point

    def has_binding(self, access_point: AccessPointKey) -> bool:
        return access_point in self._binding_by_access_point

    def runtime_state(self, access_point: AccessPointKey) -> str:
        binding = self._binding_by_access_point.get(access_point)
        if binding is None:
            return "UNBOUND"
        if binding.driver is None:
            return "BOUND_IDLE"
        return "RUNNING"

    def get_binding_info(self, access_point: AccessPointKey) -> dict[str, str] | None:
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
        binding = self._binding_by_access_point.get(access_point)
        if binding is None:
            return False
        if binding.driver is None:
            return True
        binding.driver.stop()
        binding.driver = None
        return True

    def start_bound_agent(self, access_point: AccessPointKey) -> dict[str, Any]:
        binding = self._binding_by_access_point.get(access_point)
        if binding is None:
            raise RuntimeError("no bound agent for this access point")
        if binding.driver is not None:
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
        restored_id = agent_id.strip() or (
            f"restored-agent-{access_point.type}-"
            f"{access_point.chat_id}-"
            f"{access_point.thread_id if access_point.thread_id is not None else 'main'}"
        )
        binding = _InteractiveAgentBinding(
            node_id=restored_id,
            cwd=str(resolved_spec.get("cwd") or ""),
            model=str(resolved_spec.get("model") or ""),
            mode=str(resolved_spec.get("mode") or "proxy"),
            spec=resolved_spec,
            thread_id=str(resolved_spec.get("thread_id") or ""),
            requested_thread_id=str(resolved_spec.get("thread_id") or ""),
            driver=None,
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
        snapshot: dict[AccessPointKey, _PersistedAccessPointState] = {}
        for access_point, binding in self._binding_by_access_point.items():
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
        binding = self._binding_by_access_point.pop(access_point, None)
        if binding is None:
            return False
        if binding.driver is not None:
            binding.driver.stop()
        return True

    def submit_request(
        self,
        access_point: AccessPointKey,
        text: str,
        *,
        context_note: str | None = None,
    ) -> None:
        _ = context_note
        binding = self._binding_by_access_point.get(access_point)
        if binding is None or binding.driver is None:
            raise RuntimeError("no running agent is bound to this access point")
        self._logger.event(
            "runtime_agent_submit_request",
            access_point_type=access_point.type,
            chat_id=access_point.chat_id,
            thread_id=access_point.thread_id,
            agent_id=binding.node_id,
            runtime_thread_id=binding.thread_id,
            text_preview=str(text).strip()[:200],
        )
        binding.driver.submit_request(
            _TelegramAgentRequest(
                chat_id=access_point.chat_id,
                thread_id=access_point.thread_id,
                prompt=str(text),
            )
        )

    def submit_approval_decision(self, access_point: AccessPointKey, decision: str) -> None:
        binding = self._binding_by_access_point.get(access_point)
        if binding is None or binding.driver is None:
            raise RuntimeError("no running agent is bound to this access point")
        self._logger.event(
            "runtime_agent_submit_approval_decision",
            access_point_type=access_point.type,
            chat_id=access_point.chat_id,
            thread_id=access_point.thread_id,
            agent_id=binding.node_id,
            runtime_thread_id=binding.thread_id,
            decision=decision,
        )
        binding.driver.submit_approval_decision(decision)

    def poll_once(self) -> list[StewardDriverEvent]:
        out: list[StewardDriverEvent] = []
        for access_point, binding in list(self._binding_by_access_point.items()):
            driver = binding.driver
            if driver is None:
                continue
            for event in driver.poll_once():
                self._logger.event(
                    "runtime_agent_driver_event",
                    access_point_type=access_point.type,
                    chat_id=access_point.chat_id,
                    thread_id=access_point.thread_id,
                    agent_id=binding.node_id,
                    runtime_thread_id=binding.thread_id,
                    event_type=type(event).__name__,
                )
                out.append(StewardDriverEvent(access_point=access_point, source="agent", event=event))
            if driver.is_ready():
                actual_model = driver.get_actual_thread_model().strip()
                if actual_model:
                    binding.model = actual_model
                    binding.spec["model"] = actual_model
                actual_thread_id = driver.get_thread_id().strip()
                if actual_thread_id:
                    binding.thread_id = actual_thread_id
                    binding.spec["thread_id"] = actual_thread_id
                if not binding.startup_logged:
                    lifecycle_event = "thread_resumed" if binding.requested_thread_id else "thread_started"
                    self._logger.event(
                        lifecycle_event,
                        access_point_type=access_point.type,
                        chat_id=access_point.chat_id,
                        thread_id=access_point.thread_id,
                        agent_id=binding.node_id,
                        runtime_thread_id=binding.thread_id,
                        requested_thread_id=binding.requested_thread_id,
                        mode=binding.mode,
                        model=binding.model,
                        cwd=binding.cwd,
                    )
                    binding.startup_logged = True
        return out

    def show_running(self, access_point: AccessPointKey) -> list[dict[str, str]]:
        binding = self._binding_by_access_point.get(access_point)
        if binding is None or binding.driver is None:
            return []
        return [
            {
                "agent_id": binding.node_id,
                "state": "running",
                "cwd": binding.cwd,
                "model": binding.model,
                "mode": binding.mode,
                "thread_id": binding.thread_id,
            }
        ]

    def get_item_status_snapshot(self, access_point: AccessPointKey) -> list[dict[str, str]]:
        binding = self._binding_by_access_point.get(access_point)
        if binding is None or binding.driver is None:
            return []
        return binding.driver.get_item_status_snapshot()

    def get_item_status_snapshot_for_turn(self, access_point: AccessPointKey, turn_id: str) -> list[dict[str, str]]:
        binding = self._binding_by_access_point.get(access_point)
        if binding is None or binding.driver is None:
            return []
        return binding.driver.get_item_status_snapshot_for_turn(turn_id)

    def get_last_item_apply_info(self, access_point: AccessPointKey) -> dict | None:
        binding = self._binding_by_access_point.get(access_point)
        if binding is None or binding.driver is None:
            return None
        return binding.driver.get_last_item_apply_info()

    def close(self) -> None:
        bindings = list(self._binding_by_access_point.values())
        self._binding_by_access_point.clear()
        for binding in bindings:
            if binding.driver is not None:
                binding.driver.stop()

    def _build_driver(self, *, access_point: AccessPointKey, spec: dict[str, Any]) -> TelegramInteractiveCodexDriver:
        if self._driver_factory is not None:
            return self._driver_factory(access_point, spec, self._logger)

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

        command = list(self._agent_command)
        raw_args = spec.get("args") or []
        if isinstance(raw_args, list):
            command.extend(str(item) for item in raw_args)
        model = str(spec.get("model") or "").strip()
        if model == "gpt-5-codex" and not _has_model_reasoning_effort_override(command):
            command.extend(["-c", 'model_reasoning_effort="high"'])
            self._logger.event(
                "runtime_agent_command_amended",
                access_point_type=access_point.type,
                chat_id=access_point.chat_id,
                thread_id=access_point.thread_id,
                amendment='model_reasoning_effort="high"',
                model=model,
            )
        thread_approval_policy = str(spec.get("approval_policy") or self._default_thread_approval_policy)
        thread_sandbox = str(spec.get("sandbox") or self._default_thread_sandbox)
        return TelegramInteractiveCodexDriver(
            command=command,
            logger=self._logger,
            request_timeout_sec=self._request_timeout_sec,
            rpc_timeout_sec=self._rpc_timeout_sec,
            rpc_retries=self._rpc_retries,
            approval_policy=self._approval_policy,
            thread_approval_policy=thread_approval_policy,
            thread_sandbox=thread_sandbox,
            resume_thread_id=(str(spec.get("thread_id") or "").strip() or None),
            working_dir=str(spec.get("cwd") or ""),
            initial_model=model or None,
            client_factory=self._client_factory,
            edge_thread_name=self._edge_thread_name,
        )


class StopStewardLoop(Exception):
    def __init__(self, code: int, *, drain: bool) -> None:
        super().__init__(code)
        self.code = code
        self.drain = drain


class StewardTickLoop:
    def __init__(
        self,
        *,
        request_timeout_sec: float,
        drain_driver_events: Callable[[], bool],
        flush_due_status_runtimes: Callable[[], bool],
        consume_transport: Callable[[], bool],
        drain_pending_inputs: Callable[[], bool],
        is_idle: Callable[[], bool],
        drain_sleep_sec: float = 0.02,
    ) -> None:
        self._request_timeout_sec = request_timeout_sec
        self._drain_driver_events = drain_driver_events
        self._flush_due_status_runtimes = flush_due_status_runtimes
        self._consume_transport = consume_transport
        self._drain_pending_inputs = drain_pending_inputs
        self._is_idle = is_idle
        self._drain_sleep_sec = drain_sleep_sec

    def tick_once(self) -> bool:
        progressed = False
        progressed = self._drain_driver_events() or progressed
        progressed = self._flush_due_status_runtimes() or progressed
        progressed = self._consume_transport() or progressed
        progressed = self._drain_pending_inputs() or progressed
        progressed = self._flush_due_status_runtimes() or progressed
        return progressed

    def tick_without_transport(self) -> bool:
        progressed = False
        progressed = self._drain_driver_events() or progressed
        progressed = self._flush_due_status_runtimes() or progressed
        progressed = self._drain_pending_inputs() or progressed
        progressed = self._flush_due_status_runtimes() or progressed
        return progressed

    def drain_until_idle(self, *, consume_transport: bool = True) -> int:
        idle_deadline = clock.monotonic() + max(0.5, float(self._request_timeout_sec) + 0.5)
        while clock.monotonic() < idle_deadline:
            try:
                progressed = self.tick_once() if consume_transport else self.tick_without_transport()
            except StopStewardLoop as stop:
                return stop.code
            if self._is_idle() and not progressed:
                return 0
            if progressed:
                idle_deadline = clock.monotonic() + max(0.5, float(self._request_timeout_sec) + 0.5)
            time.sleep(self._drain_sleep_sec)
        return 0
