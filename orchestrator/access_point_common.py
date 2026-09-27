"""Common transport-normalized access-point primitives.

This module is intentionally domain-agnostic. Product-specific layers
(`proxy`, `steward`, `orchestrator`) can build their own routing semantics
on top of these shared inbound action shapes.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
import math
from typing import Any, Callable, Generic, Hashable, Protocol, TypeVar
import urllib.error

AccessPointKeyT = TypeVar("AccessPointKeyT", bound=Hashable)
AccessPointId = int | str


@dataclass(frozen=True)
class AccessPointKey:
    type: str
    chat_id: AccessPointId
    thread_id: AccessPointId | None


@dataclass(frozen=True)
class AccessPointMessageRef:
    access_point: AccessPointKey
    transport_id: AccessPointId


def access_point_sort_key(access_point: AccessPointKey) -> tuple[str, str, str]:
    thread_id = "main" if access_point.thread_id is None else str(access_point.thread_id)
    return (access_point.type, str(access_point.chat_id), thread_id)


def format_access_point(access_point: AccessPointKey) -> str:
    thread_id = access_point.thread_id if access_point.thread_id is not None else "main"
    if access_point.type == "slack":
        return f"slack channel_id={access_point.chat_id} thread_ts={thread_id}"
    return f"{access_point.type} chat_id={access_point.chat_id} thread_id={thread_id}"


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
ACCESS_POINT_OUTBOUND_CLASS_APPROVAL_NOTICE = -5
ACCESS_POINT_OUTBOUND_CLASS_LIFECYCLE_UPDATE = -1
ACCESS_POINT_OUTBOUND_CLASS_SEND = 0
ACCESS_POINT_OUTBOUND_CLASS_EDIT = 10
ACCESS_POINT_DELIVERY_RETRY_BACKOFF_SEC = (1.0, 10.0, 30.0, 60.0)
ACCESS_POINT_DELIVERY_RETRY_REPEAT_SEC = 600.0


class AccessPointRetryPolicy(StrEnum):
    DURABLE = "durable"
    SUPERSEDABLE = "supersedable"
    EPHEMERAL = "ephemeral"


class AccessPointTransportError(RuntimeError):
    """Transport failure with structured retry metadata."""

    def __init__(
        self,
        message: str,
        *,
        retryable: bool,
        error_class: str,
        retry_after_sec: float | None = None,
        status_code: int | None = None,
    ) -> None:
        super().__init__(message)
        self.retryable = bool(retryable)
        self.error_class = str(error_class)
        self.retry_after_sec = retry_after_sec
        self.status_code = status_code


@dataclass(frozen=True)
class AccessPointDeliveryFailure:
    retryable: bool
    error_class: str
    retry_after_sec: float | None = None


@dataclass(frozen=True)
class AccessPointOutboundFailureDecision:
    retry_scheduled: bool
    error_class: str
    retry_delay_sec: float | None = None


@dataclass(frozen=True)
class AccessPointAbandonedSummary:
    count: int
    access_points: tuple[AccessPointKey, ...]


@dataclass
class QueuedAccessPointOutbound(Generic[DeliveryResultT]):
    queue_token: int
    priority: int
    sequence: int
    operation: str | None
    coalesce_key: tuple[Any, ...] | None
    progress_on_success: bool
    execute: Callable[[], DeliveryResultT]
    on_success: Callable[[DeliveryResultT], None] | None
    on_failure: Callable[[Exception], None] | None
    telemetry: dict[str, Any] | None = None
    ordering_key: Hashable | None = None
    attempt_count: int = 0
    retry_not_before: float = 0.0
    retry_wakeable: bool = True
    retry_policy: AccessPointRetryPolicy = AccessPointRetryPolicy.DURABLE
    expires_at: float | None = None
    ttl_sec: float | None = None
    replace_payload: Callable[[object], None] | None = None


class AccessPointOutboundQueue(Generic[DeliveryResultT]):
    """Single-threaded storage and state transitions for outbound delivery."""

    def __init__(self) -> None:
        self._items: list[QueuedAccessPointOutbound[DeliveryResultT]] = []
        self._coalesce_owners: dict[tuple[Any, ...], int] = {}
        self._next_queue_token = 1
        self._next_sequence = 1
        self._peak_count = 0

    def __bool__(self) -> bool:
        return bool(self._items)

    @property
    def count(self) -> int:
        return len(self._items)

    @property
    def peak_count(self) -> int:
        return self._peak_count

    def snapshot(self) -> list[QueuedAccessPointOutbound[DeliveryResultT]]:
        return list(self._items)

    def contains_coalesce_key(self, key: tuple[Any, ...]) -> bool:
        return key in self._coalesce_owners

    def enqueue(
        self,
        *,
        priority: int,
        operation: str | None,
        coalesce_key: tuple[Any, ...] | None,
        progress_on_success: bool,
        execute: Callable[[], DeliveryResultT],
        on_success: Callable[[DeliveryResultT], None] | None,
        on_failure: Callable[[Exception], None] | None,
        telemetry: dict[str, Any] | None = None,
        ordering_key: Hashable | None = None,
        retry_policy: AccessPointRetryPolicy = AccessPointRetryPolicy.DURABLE,
        expires_at: float | None = None,
        ttl_sec: float | None = None,
        replace_payload: Callable[[object], None] | None = None,
        deduplicate: bool = False,
    ) -> int:
        if deduplicate and coalesce_key is not None and self.contains_coalesce_key(coalesce_key):
            return 0
        queue_token = self._next_queue_token
        self._next_queue_token += 1
        item = QueuedAccessPointOutbound(
            queue_token=queue_token,
            priority=int(priority),
            sequence=self._next_sequence,
            operation=operation,
            coalesce_key=coalesce_key,
            progress_on_success=bool(progress_on_success),
            execute=execute,
            on_success=on_success,
            on_failure=on_failure,
            telemetry=dict(telemetry) if isinstance(telemetry, dict) else None,
            ordering_key=ordering_key,
            retry_policy=retry_policy,
            expires_at=expires_at,
            ttl_sec=ttl_sec,
            replace_payload=replace_payload,
        )
        self._next_sequence += 1
        self._items.append(item)
        if deduplicate and coalesce_key is not None:
            self._coalesce_owners[coalesce_key] = queue_token
        self._peak_count = max(self._peak_count, len(self._items))
        return queue_token

    def find(self, queue_token: int) -> QueuedAccessPointOutbound[DeliveryResultT] | None:
        return next((item for item in self._items if item.queue_token == queue_token), None)

    def ready(self, *, now: float) -> list[QueuedAccessPointOutbound[DeliveryResultT]]:
        return ready_access_point_outbounds(self._items, now=now)

    def next_ready(self, *, now: float) -> QueuedAccessPointOutbound[DeliveryResultT] | None:
        ready = self.ready(now=now)
        return select_access_point_outbound(ready) if ready else None

    def complete(self, item: QueuedAccessPointOutbound[DeliveryResultT]) -> bool:
        return self._remove(item)

    def cancel(self, item: QueuedAccessPointOutbound[DeliveryResultT]) -> bool:
        return self._remove(item)

    def cancel_matching(
        self,
        predicate: Callable[[QueuedAccessPointOutbound[DeliveryResultT]], bool],
    ) -> list[QueuedAccessPointOutbound[DeliveryResultT]]:
        return self._remove_matching(predicate)

    def _remove(self, item: QueuedAccessPointOutbound[DeliveryResultT]) -> bool:
        try:
            self._items.remove(item)
        except ValueError:
            return False
        key = item.coalesce_key
        if key is not None and self._coalesce_owners.get(key) == item.queue_token:
            self._coalesce_owners.pop(key, None)
        return True

    def _remove_matching(
        self,
        predicate: Callable[[QueuedAccessPointOutbound[DeliveryResultT]], bool],
    ) -> list[QueuedAccessPointOutbound[DeliveryResultT]]:
        removed: list[QueuedAccessPointOutbound[DeliveryResultT]] = []
        for item in self.snapshot():
            if predicate(item) and self._remove(item):
                removed.append(item)
        return removed

    def expire(self, *, now: float) -> list[QueuedAccessPointOutbound[DeliveryResultT]]:
        return self._remove_matching(lambda item: is_access_point_outbound_expired(item, now=now))

    def wake_durable(self, *, now: float) -> list[QueuedAccessPointOutbound[DeliveryResultT]]:
        return wake_durable_access_point_retries(self._items, now=now)

    def abandoned_summary(self) -> AccessPointAbandonedSummary:
        pending = [
            item
            for item in self._items
            if item.retry_policy == AccessPointRetryPolicy.DURABLE
        ]
        access_points = {
            item.ordering_key
            for item in pending
            if isinstance(item.ordering_key, AccessPointKey)
        }
        return AccessPointAbandonedSummary(
            count=len(pending),
            access_points=tuple(sorted(access_points, key=access_point_sort_key)),
        )


class AccessPointEventLogger(Protocol):
    def event(self, event: str, **fields: Any) -> None: ...


def access_point_delivery_log_fields(
    item: QueuedAccessPointOutbound[Any],
) -> dict[str, object]:
    fields: dict[str, object] = {
        "queue_token": item.queue_token,
        "operation": item.operation,
        "policy": item.retry_policy.value,
    }
    if isinstance(item.ordering_key, AccessPointKey):
        fields.update(
            access_point_type=item.ordering_key.type,
            chat_id=item.ordering_key.chat_id,
            thread_id=item.ordering_key.thread_id,
        )
    return fields


def expire_access_point_outbounds(
    queue: AccessPointOutboundQueue[Any],
    *,
    now: float,
    logger: AccessPointEventLogger,
    source: str,
) -> bool:
    expired_items = queue.expire(now=now)
    for item in expired_items:
        logger.event(
            "access_point_delivery_expired",
            source=source,
            **access_point_delivery_log_fields(item),
            attempts=item.attempt_count,
            ttl_sec=item.ttl_sec,
        )
    return bool(expired_items)


def wake_durable_access_point_outbounds(
    queue: AccessPointOutboundQueue[Any],
    *,
    now: float,
    logger: AccessPointEventLogger,
    source: str,
) -> None:
    for item in queue.wake_durable(now=now):
        logger.event(
            "access_point_delivery_retry_woken",
            source=source,
            **access_point_delivery_log_fields(item),
            attempt=item.attempt_count,
            wake_reason="outbound_success",
        )


def log_abandoned_access_point_outbounds(
    queue: AccessPointOutboundQueue[Any],
    *,
    logger: AccessPointEventLogger,
    source: str,
    access_point_type: str,
) -> None:
    summary = queue.abandoned_summary()
    if summary.count == 0:
        return
    logger.event(
        "access_point_durable_deliveries_abandoned",
        source=source,
        access_point_type=access_point_type,
        count=summary.count,
        access_points=[
            {"chat_id": item.chat_id, "thread_id": item.thread_id}
            for item in summary.access_points
        ],
    )


def access_point_outbound_sort_key(item: QueuedAccessPointOutbound[Any]) -> tuple[int, int]:
    return (int(item.priority), int(item.sequence))


def select_access_point_outbound(
    items: list[QueuedAccessPointOutbound[DeliveryResultT]],
) -> QueuedAccessPointOutbound[DeliveryResultT]:
    candidate = min(items, key=access_point_outbound_sort_key)
    if candidate.priority != ACCESS_POINT_OUTBOUND_CLASS_LIFECYCLE_UPDATE:
        return candidate
    blockers = [
        item
        for item in items
        if item.sequence < candidate.sequence
        and item.ordering_key == candidate.ordering_key
        and str(item.operation or "") not in {"status", "status_flush"}
    ]
    if not blockers:
        return candidate
    return min(blockers, key=access_point_outbound_sort_key)


def ready_access_point_outbounds(
    items: list[QueuedAccessPointOutbound[DeliveryResultT]],
    *,
    now: float,
) -> list[QueuedAccessPointOutbound[DeliveryResultT]]:
    """Return due work while a delayed delivery blocks only its own AP."""

    delayed_sequences: dict[Hashable, int] = {}
    for item in items:
        if (
            item.ordering_key is None
            or item.retry_not_before <= now
            or str(item.operation or "") in {"status", "status_flush"}
        ):
            continue
        previous = delayed_sequences.get(item.ordering_key)
        if previous is None or item.sequence < previous:
            delayed_sequences[item.ordering_key] = item.sequence

    return [
        item
        for item in items
        if item.retry_not_before <= now
        and (
            item.ordering_key is None
            or item.ordering_key not in delayed_sequences
            or item.sequence < delayed_sequences[item.ordering_key]
        )
    ]


def classify_access_point_delivery_failure(exc: Exception) -> AccessPointDeliveryFailure:
    for current in _exception_chain(exc):
        if isinstance(current, AccessPointTransportError):
            return AccessPointDeliveryFailure(
                retryable=current.retryable,
                error_class=current.error_class,
                retry_after_sec=current.retry_after_sec,
            )
        if isinstance(current, urllib.error.HTTPError):
            status_code = int(current.code)
            return AccessPointDeliveryFailure(
                retryable=status_code == 429 or 500 <= status_code < 600,
                error_class=type(current).__name__,
                retry_after_sec=_http_retry_after_sec(current),
            )
        if isinstance(current, urllib.error.URLError):
            reason = current.reason
            error_class = type(reason).__name__ if isinstance(reason, BaseException) else type(current).__name__
            return AccessPointDeliveryFailure(retryable=True, error_class=error_class)
        if isinstance(current, (TimeoutError, ConnectionError)):
            return AccessPointDeliveryFailure(
                retryable=True,
                error_class=type(current).__name__,
            )

    return AccessPointDeliveryFailure(
        retryable=False,
        error_class=type(exc).__name__,
    )


def access_point_retry_after_seconds(exc: Exception) -> int | None:
    retry_after_sec = classify_access_point_delivery_failure(exc).retry_after_sec
    if retry_after_sec is None or retry_after_sec <= 0:
        return None
    return max(1, int(math.ceil(retry_after_sec)))


def decide_access_point_outbound_failure(
    item: QueuedAccessPointOutbound[Any],
    exc: Exception,
    *,
    now: float,
) -> AccessPointOutboundFailureDecision:
    failure = classify_access_point_delivery_failure(exc)
    retryable = failure.retryable
    if str(item.operation or "") in {"status", "status_flush"} and failure.retry_after_sec is None:
        retryable = False
    if not retryable:
        return AccessPointOutboundFailureDecision(
            retry_scheduled=False,
            error_class=failure.error_class,
        )
    delay = failure.retry_after_sec
    if delay is None or delay <= 0:
        item.retry_wakeable = True
        backoff_index = max(0, item.attempt_count - 1)
        if backoff_index < len(ACCESS_POINT_DELIVERY_RETRY_BACKOFF_SEC):
            delay = ACCESS_POINT_DELIVERY_RETRY_BACKOFF_SEC[backoff_index]
    else:
        item.retry_wakeable = False
    if delay is None:
        delay = ACCESS_POINT_DELIVERY_RETRY_REPEAT_SEC
    delay = float(delay)
    item.retry_not_before = float(now) + delay
    return AccessPointOutboundFailureDecision(
        retry_scheduled=True,
        error_class=failure.error_class,
        retry_delay_sec=delay,
    )


def is_access_point_outbound_expired(
    item: QueuedAccessPointOutbound[Any],
    *,
    now: float,
) -> bool:
    return (
        item.retry_policy == AccessPointRetryPolicy.EPHEMERAL
        and item.expires_at is not None
        and float(now) >= float(item.expires_at)
    )


def wake_durable_access_point_retries(
    items: list[QueuedAccessPointOutbound[DeliveryResultT]],
    *,
    now: float,
) -> list[QueuedAccessPointOutbound[DeliveryResultT]]:
    woken: list[QueuedAccessPointOutbound[DeliveryResultT]] = []
    for item in items:
        if (
            item.retry_policy != AccessPointRetryPolicy.DURABLE
            or item.attempt_count <= 0
            or item.retry_not_before <= now
            or not item.retry_wakeable
        ):
            continue
        item.retry_not_before = float(now)
        woken.append(item)
    return woken


def _exception_chain(exc: Exception) -> list[BaseException]:
    chain: list[BaseException] = []
    seen: set[int] = set()
    current: BaseException | None = exc
    while current is not None and id(current) not in seen:
        seen.add(id(current))
        chain.append(current)
        current = current.__cause__ or current.__context__
    return chain


def _http_retry_after_sec(exc: urllib.error.HTTPError) -> float | None:
    headers = exc.headers
    if headers is None:
        return None
    raw = headers.get("Retry-After")
    if raw is None:
        return None
    try:
        return float(raw)
    except (TypeError, ValueError):
        return None


class AccessPointStatusSurface(Protocol):
    def apply_protocol_event(self, *, method: str, params: dict) -> None: ...
    def flush_due_statuses(self) -> bool: ...
    def split_current_turn(self) -> None: ...
