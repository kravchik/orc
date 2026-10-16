"""Bounded asynchronous ownership for Steward session catalog commands."""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
import hashlib
import json
from queue import Empty, Full, Queue
import re
import threading
import time
from typing import Any, Callable, Generic, Protocol, TypeVar

from orchestrator.access_point_common import AccessPointKey, AccessPointMessageRef
from orchestrator.interaction_queue import InteractionTickContext
from orchestrator.local_command_journal import LocalCommandOutcome


ResultT = TypeVar("ResultT")
_STOP = object()
_CATALOG_PROGRESS_REF_RE = re.compile(r"[0-9a-f]{64}")
_MAX_CATALOG_INTERACTION_SEQUENCE = (1 << 63) - 1
_MAX_CATALOG_OPERATION_ORDINAL = 255


class CatalogInteractionKind(StrEnum):
    AGENTS = "agents"
    RESUME = "resume"
    STEWARD_ACTION = "steward_action"


class CatalogOperationProgressPhase(StrEnum):
    STARTED = "started"
    SETTLED = "settled"


def catalog_access_point_owner_ref(access_point: AccessPointKey) -> str:
    """Return a fixed-size correlation ref without exposing access-point IDs."""

    canonical = json.dumps(
        {
            "type": access_point.type,
            "chat_id": access_point.chat_id,
            "thread_id": access_point.thread_id,
        },
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    )
    return hashlib.sha256(
        b"orc1/catalog-access-point/v1\0" + canonical.encode("utf-8")
    ).hexdigest()


@dataclass(frozen=True)
class CatalogOperationProgressContext:
    interaction_kind: CatalogInteractionKind
    access_point_owner_ref: str
    interaction_sequence: int
    operation_ordinal: int

    def __post_init__(self) -> None:
        if not isinstance(self.interaction_kind, CatalogInteractionKind):
            raise TypeError("catalog progress interaction_kind is invalid")
        if not _CATALOG_PROGRESS_REF_RE.fullmatch(self.access_point_owner_ref):
            raise ValueError("catalog progress access-point owner ref is invalid")
        if (
            isinstance(self.interaction_sequence, bool)
            or not isinstance(self.interaction_sequence, int)
            or self.interaction_sequence <= 0
            or self.interaction_sequence > _MAX_CATALOG_INTERACTION_SEQUENCE
        ):
            raise ValueError("catalog progress interaction sequence is invalid")
        if (
            isinstance(self.operation_ordinal, bool)
            or not isinstance(self.operation_ordinal, int)
            or self.operation_ordinal < 0
            or self.operation_ordinal > _MAX_CATALOG_OPERATION_ORDINAL
        ):
            raise ValueError("catalog progress operation ordinal is invalid")

    @property
    def operation_ref(self) -> str:
        canonical = json.dumps(
            {
                "access_point_owner_ref": self.access_point_owner_ref,
                "interaction_kind": self.interaction_kind.value,
                "interaction_sequence": self.interaction_sequence,
                "operation_ordinal": self.operation_ordinal,
            },
            ensure_ascii=True,
            separators=(",", ":"),
            sort_keys=True,
        )
        return hashlib.sha256(
            b"orc1/catalog-operation/v1\0" + canonical.encode("utf-8")
        ).hexdigest()


@dataclass(frozen=True)
class CatalogOperationProgress:
    phase: CatalogOperationProgressPhase
    context: CatalogOperationProgressContext

    def lifecycle_fields(self) -> dict[str, object]:
        return {
            "phase": self.phase.value,
            "interaction_kind": self.context.interaction_kind.value,
            "access_point_owner_ref": self.context.access_point_owner_ref,
            "interaction_sequence": self.context.interaction_sequence,
            "operation_ordinal": self.context.operation_ordinal,
            "operation_ref": self.context.operation_ref,
        }


class CatalogOperationCapacityError(RuntimeError):
    pass


class CatalogOperationHandle(Generic[ResultT]):
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._done = False
        self._settled = False
        self._cancelled = False
        self._taken = False
        self._result: ResultT | None = None
        self._error: Exception | None = None

    @property
    def done(self) -> bool:
        with self._lock:
            return self._done

    @property
    def cancelled(self) -> bool:
        with self._lock:
            return self._cancelled

    @property
    def settled(self) -> bool:
        with self._lock:
            return self._settled

    def cancel(self) -> bool:
        with self._lock:
            if self._done:
                return False
            self._cancelled = True
            self._done = True
            return True

    def take_result(self) -> ResultT | None:
        with self._lock:
            if not self._done:
                raise RuntimeError("catalog operation is still pending")
            if self._taken:
                raise RuntimeError("catalog operation result was already consumed")
            self._taken = True
            if self._cancelled:
                return None
            if self._error is not None:
                raise self._error
            return self._result

    def _begin(self) -> bool:
        with self._lock:
            if self._cancelled:
                return False
            return True

    def _complete(self, result: ResultT) -> None:
        with self._lock:
            if self._cancelled:
                return
            self._result = result
            self._done = True

    def _fail(self, error: Exception) -> None:
        with self._lock:
            if self._cancelled:
                return
            self._error = error
            self._done = True

    def _settle(self) -> None:
        with self._lock:
            self._settled = True

    @classmethod
    def completed(cls, result: ResultT) -> CatalogOperationHandle[ResultT]:
        handle: CatalogOperationHandle[ResultT] = cls()
        handle._complete(result)
        handle._settle()
        return handle


class CatalogOperationExecutor:
    def __init__(
        self,
        *,
        max_workers: int = 8,
        max_pending: int = 32,
        thread_name_prefix: str = "steward-catalog",
        operation_progress: Callable[[CatalogOperationProgress], None] | None = None,
    ) -> None:
        if max_workers <= 0 or max_workers > 8:
            raise ValueError("catalog max_workers must be between 1 and 8")
        if max_pending < max_workers or max_pending > 64:
            raise ValueError("catalog max_pending must cover workers and stay bounded")
        prefix = str(thread_name_prefix).strip()
        if not prefix or len(prefix) > 64:
            raise ValueError("catalog thread_name_prefix is invalid")
        self._max_workers = max_workers
        self._queue: Queue[
            tuple[
                CatalogOperationHandle[Any],
                Callable[[], Any],
                object | None,
                CatalogOperationProgressContext | None,
            ]
            | object
        ] = Queue(maxsize=max_pending)
        self._thread_name_prefix = prefix
        self._operation_progress = operation_progress
        self._lock = threading.Lock()
        self._threads: list[threading.Thread] = []
        self._closed = False
        self._stop_enqueued = False
        self._admission_owners: set[object] = set()

    def submit(
        self,
        operation: Callable[[], ResultT],
        *,
        admission_owner: object | None = None,
        progress_context: CatalogOperationProgressContext | None = None,
    ) -> CatalogOperationHandle[ResultT] | None:
        if not callable(operation):
            raise TypeError("catalog operation must be callable")
        if self._operation_progress is not None and not isinstance(
            progress_context,
            CatalogOperationProgressContext,
        ):
            raise TypeError("catalog operation progress context is required")
        if self._operation_progress is None and progress_context is not None:
            raise ValueError("catalog operation progress context has no observer")
        if admission_owner is not None:
            try:
                hash(admission_owner)
            except TypeError as exc:
                raise TypeError("catalog admission owner must be hashable") from exc
        handle: CatalogOperationHandle[ResultT] = CatalogOperationHandle()
        with self._lock:
            if self._closed:
                raise RuntimeError("catalog operation executor is closed")
            if (
                admission_owner is not None
                and admission_owner in self._admission_owners
            ):
                return None
            if admission_owner is not None:
                self._admission_owners.add(admission_owner)
            self._start_workers_locked()
            if self._queue.full():
                self._release_admission_locked(admission_owner)
                raise CatalogOperationCapacityError(
                    "catalog operation capacity is exhausted"
                )
            self._emit_operation_progress(
                CatalogOperationProgressPhase.STARTED,
                progress_context,
            )
            try:
                self._queue.put_nowait(
                    (handle, operation, admission_owner, progress_context)
                )
            except Full as exc:
                self._release_admission_locked(admission_owner)
                self._emit_operation_progress(
                    CatalogOperationProgressPhase.SETTLED,
                    progress_context,
                )
                raise CatalogOperationCapacityError(
                    "catalog operation capacity is exhausted"
                ) from exc
        return handle

    def close(self, *, timeout_sec: float = 5.0) -> None:
        timeout = float(timeout_sec)
        if timeout <= 0 or timeout > 30:
            raise ValueError("catalog executor cleanup timeout is invalid")
        with self._lock:
            self._closed = True
            threads = tuple(self._threads)
            if not self._stop_enqueued:
                while True:
                    try:
                        queued = self._queue.get_nowait()
                    except Empty:
                        break
                    if queued is not _STOP:
                        handle, _operation, admission_owner, progress_context = queued
                        handle.cancel()
                        self._release_admission_locked(admission_owner)
                        handle._settle()
                        self._emit_operation_progress(
                            CatalogOperationProgressPhase.SETTLED,
                            progress_context,
                        )
                    self._queue.task_done()
                for _thread in threads:
                    self._queue.put_nowait(_STOP)
                self._stop_enqueued = True
        deadline = time.monotonic() + timeout
        for thread in threads:
            thread.join(max(0.0, deadline - time.monotonic()))
        alive = [thread.name for thread in threads if thread.is_alive()]
        if alive:
            raise RuntimeError(
                f"catalog operation workers did not stop within {timeout:.3f}s"
            )

    def _start_workers_locked(self) -> None:
        if self._threads:
            return
        for index in range(1, self._max_workers + 1):
            thread = threading.Thread(
                target=self._run,
                name=f"{self._thread_name_prefix}-{index}",
                daemon=False,
            )
            self._threads.append(thread)
            thread.start()

    def _run(self) -> None:
        while True:
            queued = self._queue.get()
            try:
                if queued is _STOP:
                    return
                handle, operation, admission_owner, progress_context = queued
                if not handle._begin():
                    continue
                try:
                    result = operation()
                except Exception as exc:
                    handle._fail(exc)
                else:
                    handle._complete(result)
            finally:
                if queued is not _STOP:
                    with self._lock:
                        self._release_admission_locked(admission_owner)
                    handle._settle()
                    self._emit_operation_progress(
                        CatalogOperationProgressPhase.SETTLED,
                        progress_context,
                    )
                self._queue.task_done()

    def _emit_operation_progress(
        self,
        phase: CatalogOperationProgressPhase,
        context: CatalogOperationProgressContext | None,
    ) -> None:
        if self._operation_progress is None:
            return
        if context is None:
            raise AssertionError("catalog operation progress context is missing")
        try:
            self._operation_progress(CatalogOperationProgress(phase, context))
        except Exception:
            return

    def _release_admission_locked(self, admission_owner: object | None) -> None:
        if admission_owner is not None:
            self._admission_owners.discard(admission_owner)


class AgentCatalogInteractionRuntime(Protocol):
    def catalog_interaction_tick(self, interaction: AgentCatalogInteraction) -> str | None: ...
    def catalog_interaction_completed(
        self,
        interaction: AgentCatalogInteraction,
        outcome: str,
    ) -> None: ...


class AgentResumeInteractionRuntime(Protocol):
    def resume_interaction_tick(self, interaction: AgentResumeInteraction) -> str | None: ...
    def resume_interaction_completed(
        self,
        interaction: AgentResumeInteraction,
        outcome: str,
    ) -> None: ...


@dataclass(eq=False)
class AgentCatalogInteraction:
    access_point: AccessPointKey
    interaction_sequence: int
    raw_command: str
    scope: str
    target_cwd: str
    message_ref: AccessPointMessageRef | None = None
    operations_started: bool = False
    operation_profiles: tuple[Any, ...] = ()
    operations: tuple[CatalogOperationHandle[Any], ...] = ()
    operation_started_at: float | None = None
    final_texts: tuple[str, ...] = ()
    outcome: LocalCommandOutcome | None = None
    kind: Any = None
    edit_delivered: bool = False
    pending_followups: int = 0
    delivery_failed: bool = False
    finished: bool = False
    terminal_reason: str = ""

    def tick(self, context: InteractionTickContext) -> str | None:
        runtime: AgentCatalogInteractionRuntime = context.runtime
        before = (
            self.operations_started,
            self.operations,
            self.final_texts,
            self.edit_delivered,
            self.pending_followups,
            self.delivery_failed,
            self.finished,
        )
        outcome = runtime.catalog_interaction_tick(self)
        after = (
            self.operations_started,
            self.operations,
            self.final_texts,
            self.edit_delivered,
            self.pending_followups,
            self.delivery_failed,
            self.finished,
        )
        if before != after:
            context.progressed = True
        return outcome

    def on_completed(self, context: InteractionTickContext, outcome: str) -> None:
        runtime: AgentCatalogInteractionRuntime = context.runtime
        runtime.catalog_interaction_completed(self, outcome)


@dataclass(eq=False)
class AgentResumeInteraction:
    access_point: AccessPointKey
    interaction_sequence: int
    raw_command: str
    scope: str
    selector: str
    initial_agent_state: str
    initial_block_reason: str = ""
    target_cwd: str = ""
    message_ref: AccessPointMessageRef | None = None
    phase: str = "awaiting_status"
    operations_started: bool = False
    operations: tuple[CatalogOperationHandle[Any], ...] = ()
    operation_started_at: float | None = None
    selected: Any = None
    catalog_failures: tuple[Any, ...] = ()
    projected_session_id: str = ""
    agent_id: str = ""
    edit_pending: bool = False
    edit_purpose: str = ""
    edit_text: str = ""
    final_texts: tuple[str, ...] = ()
    outcome: LocalCommandOutcome | None = None
    kind: Any = None
    pending_followups: int = 0
    delivery_failed: bool = False
    finished: bool = False
    completion_recorded: bool = False
    terminal_reason: str = ""

    def tick(self, context: InteractionTickContext) -> str | None:
        runtime: AgentResumeInteractionRuntime = context.runtime
        before = (
            self.phase,
            self.operations_started,
            self.operations,
            self.selected,
            self.agent_id,
            self.edit_pending,
            self.final_texts,
            self.pending_followups,
            self.delivery_failed,
            self.finished,
            self.completion_recorded,
        )
        outcome = runtime.resume_interaction_tick(self)
        after = (
            self.phase,
            self.operations_started,
            self.operations,
            self.selected,
            self.agent_id,
            self.edit_pending,
            self.final_texts,
            self.pending_followups,
            self.delivery_failed,
            self.finished,
            self.completion_recorded,
        )
        if before != after:
            context.progressed = True
        return outcome

    def on_completed(self, context: InteractionTickContext, outcome: str) -> None:
        runtime: AgentResumeInteractionRuntime = context.runtime
        runtime.resume_interaction_completed(self, outcome)
