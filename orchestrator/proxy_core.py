"""Shared proxy-style core lifecycle above access-point-specific adapters."""

from __future__ import annotations

from typing import Any, Generic, Hashable, Protocol, TypeVar

from orchestrator.processes import LifecycleLogger
from orchestrator.proxy_access_point import ProxyAccessPointAdapter
from orchestrator.proxy_core_base import ProxyCoreBase
from orchestrator.turn_status_store import TurnStatusStore


AccessPointKeyT = TypeVar("AccessPointKeyT", bound=Hashable)
OutputRuntimeT = TypeVar("OutputRuntimeT")
DriverT = TypeVar("DriverT")
ResultT = TypeVar("ResultT")
ApprovalEventT = TypeVar("ApprovalEventT")
OutboundNoteT = TypeVar("OutboundNoteT")
StatusEventT = TypeVar("StatusEventT")
PendingApprovalT = TypeVar("PendingApprovalT")


class Writer(Protocol):
    def __call__(self, text: str) -> object: ...


class ProxyCore(
    ProxyCoreBase[AccessPointKeyT, OutputRuntimeT, DriverT],
    Generic[
        AccessPointKeyT,
        OutputRuntimeT,
        DriverT,
        ResultT,
        ApprovalEventT,
        OutboundNoteT,
        StatusEventT,
        PendingApprovalT,
    ],
):
    """Common proxy lifecycle for results, approvals, notes, and status events."""

    def __init__(
        self,
        *,
        logger: LifecycleLogger,
        source: str,
        driver: DriverT,
        writer: Writer,
    ) -> None:
        super().__init__(logger=logger, source=source, driver=driver)
        self._writer = writer
        self._pending_approval: PendingApprovalT | None = None
        self._access_point: ProxyAccessPointAdapter[
            AccessPointKeyT,
            OutputRuntimeT,
            ResultT,
            ApprovalEventT,
            OutboundNoteT,
            StatusEventT,
            PendingApprovalT,
        ] | None = None

    def bind_access_point(
        self,
        access_point: ProxyAccessPointAdapter[
            AccessPointKeyT,
            OutputRuntimeT,
            ResultT,
            ApprovalEventT,
            OutboundNoteT,
            StatusEventT,
            PendingApprovalT,
        ],
    ) -> None:
        self._access_point = access_point

    def _require_access_point(
        self,
    ) -> ProxyAccessPointAdapter[
        AccessPointKeyT,
        OutputRuntimeT,
        ResultT,
        ApprovalEventT,
        OutboundNoteT,
        StatusEventT,
        PendingApprovalT,
    ]:
        if self._access_point is None:
            raise RuntimeError("proxy access point is not bound")
        return self._access_point

    @property
    def pending_approval(self) -> PendingApprovalT | None:
        return self._pending_approval

    def record_result(self, result: ResultT) -> None:
        self.set_awaiting(True)
        self._send_result(result)
        self._after_record_result(result)
        self._writer(self._format_result_log(result))

    def register_approval_request(self, event: ApprovalEventT) -> None:
        self._pending_approval = self._send_approval_prompt(event)

    def clear_pending_approval(self) -> None:
        self._pending_approval = None

    def emit_outbound_note(self, note: OutboundNoteT) -> None:
        self._send_outbound_note(note)

    def flush_delivery_lane(self) -> bool:
        return bool(self._require_access_point().flush_delivery_lane())

    def apply_status_update(self, event: StatusEventT) -> None:
        self.apply_status_text(
            access_point=self._access_point_for_status_event(event),
            status_text=self._status_text_for_event(event),
        )

    def _send_result(self, result: ResultT) -> None:
        self._require_access_point().send_result(result)

    def _after_record_result(self, result: ResultT) -> None:
        self._require_access_point().after_record_result(result)

    def _format_result_log(self, result: ResultT) -> str:
        return self._require_access_point().format_result_log(result)

    def _send_approval_prompt(self, event: ApprovalEventT) -> PendingApprovalT:
        return self._require_access_point().send_approval_prompt(event)

    def _send_outbound_note(self, note: OutboundNoteT) -> None:
        self._require_access_point().send_outbound_note(note)

    def _access_point_for_status_event(self, event: StatusEventT) -> AccessPointKeyT:
        return self._require_access_point().access_point_for_status_event(event)

    def _status_text_for_event(self, event: StatusEventT) -> str:
        return self._require_access_point().status_text_for_event(event)

    def _build_status_runtime(
        self,
        access_point: AccessPointKeyT,
    ) -> tuple[OutputRuntimeT, TurnStatusStore]:
        return self._require_access_point().build_status_runtime(access_point)
