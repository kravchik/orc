"""Shared runner/bootstrap helpers for steward-style runtimes."""

from __future__ import annotations

import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Protocol

from orchestrator import clock
from orchestrator.processes import LifecycleLogger
from orchestrator.steward_runtime_support import (
    AccessPointStateStore,
    InteractiveAgentRuntime,
    StopStewardLoop,
    StewardTickLoop,
)
from orchestrator.telegram_steward_helpers import (
    AccessPointKey,
    _PersistedAccessPointState,
)


Writer = Callable[[str], None]
STEWARD_IDLE_SLEEP_SEC = 0.01
STEWARD_SLOW_TICK_WARNING_SEC = 0.1


class StewardTransportAdapter(Protocol):
    stopping_message: str
    sigint_event_name: str
    stopped_event_name: str

    def should_stop_after_tick(self) -> bool: ...
    def drain_on_forced_stop(self, loop: StewardTickLoop) -> int: ...
    def handle_stop(self, loop: StewardTickLoop, stop: StopStewardLoop) -> int: ...
    def handle_error(self, exc: Exception) -> int: ...
    def stop_resources(self) -> None: ...


@dataclass
class StewardRegistryContext:
    logger: LifecycleLogger
    agent_runtime: InteractiveAgentRuntime
    state_store: AccessPointStateStore | None
    persisted_state_by_access_point: dict[AccessPointKey, _PersistedAccessPointState]
    pending_restore_greeting: set[AccessPointKey]
    sort_key: Callable[[AccessPointKey], tuple[str, str, str]]

    def persist(self, *, reason: str, access_point: AccessPointKey | None = None) -> None:
        if self.state_store is None:
            return
        merged = dict(self.persisted_state_by_access_point)
        merged.update(self.agent_runtime.snapshot_persisted())
        pruned: dict[AccessPointKey, _PersistedAccessPointState] = {}
        for key, value in merged.items():
            has_agent = isinstance(value.agent, dict) and bool(value.agent)
            has_cwd = bool((value.project_cwd or "").strip())
            if has_agent or has_cwd:
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
            return
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

    def drop(self, access_point: AccessPointKey) -> None:
        self.persisted_state_by_access_point.pop(access_point, None)
        self.persist(reason="access_point_reset", access_point=access_point)


def build_steward_registry_context(
    *,
    logger: LifecycleLogger,
    agent_runtime: InteractiveAgentRuntime,
    state_path: str | Path | None,
    sort_key: Callable[[AccessPointKey], tuple[str, str, str]],
) -> StewardRegistryContext:
    state_store: AccessPointStateStore | None = None
    persisted_state_by_access_point: dict[AccessPointKey, _PersistedAccessPointState] = {}
    pending_restore_greeting: set[AccessPointKey] = set()
    if state_path is not None and str(state_path).strip():
        state_store = AccessPointStateStore(path=Path(str(state_path)).expanduser(), logger=logger)
        persisted_state_by_access_point = state_store.load()
        for access_point, state in list(persisted_state_by_access_point.items()):
            agent_payload = state.agent
            if not isinstance(agent_payload, dict):
                continue
            spec_payload = agent_payload.get("spec")
            spec = dict(spec_payload) if isinstance(spec_payload, dict) else {
                "cwd": str(agent_payload.get("cwd") or state.project_cwd or ""),
                "model": str(agent_payload.get("model") or "gpt-5-codex"),
                "mode": str(agent_payload.get("mode") or "proxy"),
            }
            if not str(spec.get("cwd") or "").strip():
                continue
            try:
                agent_runtime.restore_binding(
                    access_point,
                    spec=spec,
                    agent_id=str(agent_payload.get("agent_id") or ""),
                    thread_id=str(agent_payload.get("thread_id") or ""),
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
        pending_restore_greeting = set(persisted_state_by_access_point.keys())
    return StewardRegistryContext(
        logger=logger,
        agent_runtime=agent_runtime,
        state_store=state_store,
        persisted_state_by_access_point=persisted_state_by_access_point,
        pending_restore_greeting=pending_restore_greeting,
        sort_key=sort_key,
    )


def run_steward_runtime(
    *,
    loop: StewardTickLoop,
    transport: StewardTransportAdapter,
    writer: Writer,
    logger: LifecycleLogger,
    sleep_fn: Callable[[float], None] = time.sleep,
    monotonic_now: Callable[[], float] = clock.monotonic,
) -> int:
    try:
        while True:
            tick_started_at = float(monotonic_now())
            progressed = loop.tick_once()
            tick_duration_sec = float(monotonic_now()) - tick_started_at
            if tick_duration_sec >= STEWARD_SLOW_TICK_WARNING_SEC:
                logger.event(
                    "steward_slow_tick",
                    duration_ms=round(tick_duration_sec * 1000.0, 3),
                    progressed=progressed,
                )
            if transport.should_stop_after_tick():
                return transport.drain_on_forced_stop(loop)
            if not progressed:
                sleep_fn(STEWARD_IDLE_SLEEP_SEC)
    except StopStewardLoop as stop:
        return transport.handle_stop(loop, stop)
    except KeyboardInterrupt:
        writer("^C")
        writer(transport.stopping_message)
        logger.event(transport.sigint_event_name)
        return 130
    except Exception as exc:
        return transport.handle_error(exc)
    finally:
        transport.stop_resources()
        logger.event(transport.stopped_event_name)
