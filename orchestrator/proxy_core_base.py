"""Shared base for proxy-like single-agent cores across access points."""

from __future__ import annotations

import threading
from typing import Generic, Hashable, TypeVar

from orchestrator.processes import LifecycleLogger
from orchestrator.turn_status_store import TurnStatusStore


AccessPointKeyT = TypeVar("AccessPointKeyT", bound=Hashable)
OutputRuntimeT = TypeVar("OutputRuntimeT")
DriverT = TypeVar("DriverT")


class ProxyCoreBase(Generic[AccessPointKeyT, OutputRuntimeT, DriverT]):
    """Common gate and turn-status lifecycle for proxy-style access points."""

    def __init__(
        self,
        *,
        logger: LifecycleLogger,
        source: str,
        driver: DriverT,
    ) -> None:
        self._logger = logger
        self._source = source
        self._driver = driver
        self._gate_lock = threading.Lock()
        self._awaiting_human_input = False
        self._status_runtime_by_access_point: dict[
            AccessPointKeyT,
            tuple[OutputRuntimeT, TurnStatusStore],
        ] = {}

    def is_awaiting(self) -> bool:
        with self._gate_lock:
            return self._awaiting_human_input

    def set_awaiting(self, value: bool) -> None:
        with self._gate_lock:
            self._awaiting_human_input = value
        self._logger.event("human_input_gate_changed", source=self._source, awaiting=value)

    def apply_status_text(
        self,
        *,
        access_point: AccessPointKeyT,
        status_text: str,
    ) -> None:
        _output_runtime, status_store = self._status_runtime_for(access_point)
        status_store.apply_backend_status(
            status_text=status_text,
            snapshot_getter=self._driver.get_item_status_snapshot,
            snapshot_for_turn_getter=self._driver.get_item_status_snapshot_for_turn,
            apply_info_getter=self._driver.get_last_item_apply_info,
        )

    def split_status_after_approval(self, *, access_point: AccessPointKeyT) -> None:
        _output_runtime, status_store = self._status_runtime_for(access_point)
        status_store.flush_current_turn()
        status_store.split_current_turn()

    def flush_due_statuses(self) -> bool:
        progressed = False
        for output_runtime, _status_store in self._status_runtime_by_access_point.values():
            if output_runtime.flush_due_statuses():
                progressed = True
        return progressed

    def _status_runtime_for(
        self,
        access_point: AccessPointKeyT,
    ) -> tuple[OutputRuntimeT, TurnStatusStore]:
        existing = self._status_runtime_by_access_point.get(access_point)
        if existing is not None:
            return existing
        existing = self._build_status_runtime(access_point)
        self._status_runtime_by_access_point[access_point] = existing
        return existing

    def _build_status_runtime(
        self,
        access_point: AccessPointKeyT,
    ) -> tuple[OutputRuntimeT, TurnStatusStore]:
        raise NotImplementedError
