"""Common transport-normalized access-point primitives.

This module is intentionally domain-agnostic. Product-specific layers
(`proxy`, `steward`, `orchestrator`) can build their own routing semantics
on top of these shared inbound action shapes.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
from typing import Any, Callable, Generic, Hashable, Protocol, TypeVar


AccessPointKeyT = TypeVar("AccessPointKeyT", bound=Hashable)


@dataclass(frozen=True)
class AccessPointTextInput(Generic[AccessPointKeyT]):
    access_point: AccessPointKeyT
    text: str
    source: str = "transport"


@dataclass(frozen=True)
class AccessPointApprovalDecision(Generic[AccessPointKeyT]):
    access_point: AccessPointKeyT
    decision: str
    via: str = "text"
    source: str = "transport"
    callback_query_id: str | None = None
    callback_data: str | None = None


@dataclass(frozen=True)
class AccessPointApprovalDetailsRequest(Generic[AccessPointKeyT]):
    access_point: AccessPointKeyT
    source: str = "transport"
    callback_query_id: str | None = None


@dataclass(frozen=True)
class AccessPointLocalCommand(Generic[AccessPointKeyT]):
    access_point: AccessPointKeyT
    command: str
    source: str = "transport"


class AccessPointDeliveryState(StrEnum):
    PENDING = "pending"
    SENT = "sent"
    FAILED = "failed"
    CANCELLED = "cancelled"


@dataclass
class AccessPointDeliveryReceipt:
    receipt_id: int
    state: AccessPointDeliveryState = AccessPointDeliveryState.PENDING
    message_id: int | None = None
    message_ts: str | None = None
    error: str | None = None
    error_type: str | None = None

    def mark_sent(self, *, message_id: int | None = None, message_ts: str | None = None) -> None:
        self.state = AccessPointDeliveryState.SENT
        if message_id is not None:
            self.message_id = int(message_id)
        if isinstance(message_ts, str) and message_ts.strip():
            self.message_ts = message_ts.strip()
        self.error = None
        self.error_type = None

    def mark_failed(self, exc: Exception) -> None:
        self.state = AccessPointDeliveryState.FAILED
        self.error = str(exc)
        self.error_type = type(exc).__name__

    def mark_cancelled(self) -> None:
        self.state = AccessPointDeliveryState.CANCELLED


DeliveryResultT = TypeVar("DeliveryResultT")


ACCESS_POINT_OUTBOUND_CLASS_CALLBACK_ACK = -20
ACCESS_POINT_OUTBOUND_CLASS_APPROVAL_CLEANUP = -10
ACCESS_POINT_OUTBOUND_CLASS_SEND = 0
ACCESS_POINT_OUTBOUND_CLASS_EDIT = 10


@dataclass
class QueuedAccessPointOutbound(Generic[DeliveryResultT]):
    receipt_id: int
    priority: int
    sequence: int
    operation: str | None
    coalesce_key: tuple[Any, ...] | None
    progress_on_success: bool
    execute: Callable[[], DeliveryResultT]
    on_success: Callable[[DeliveryResultT], None] | None
    on_failure: Callable[[Exception], None] | None
    telemetry: dict[str, Any] | None = None


def access_point_outbound_sort_key(item: QueuedAccessPointOutbound[Any]) -> tuple[int, int]:
    return (int(item.priority), int(item.sequence))


class AccessPointStatusSurface(Protocol):
    def apply_protocol_event(self, *, method: str, params: dict) -> None: ...
    def flush_due_statuses(self) -> bool: ...
    def split_current_turn(self) -> None: ...
