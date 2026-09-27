"""Startup lifecycle notices owned by the shared interaction queue."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Protocol

from orchestrator.access_point_common import AccessPointKey, AccessPointMessageRef
from orchestrator.interactive_driver_events import InteractiveStartupResult
from orchestrator.local_command_journal import LocalCommandOutcome
from orchestrator.interaction_queue import InteractionTickContext


class LocalLifecycleRuntime(Protocol):
    def lifecycle_queue_initial(self, operation: LocalLifecycleOperation, text: str) -> int | None: ...
    def lifecycle_supersede(self, operation: LocalLifecycleOperation) -> bool: ...
    def lifecycle_queue_edit(self, operation: LocalLifecycleOperation) -> int | None: ...
    def lifecycle_supersede_edit(self, operation: LocalLifecycleOperation) -> bool: ...
    def lifecycle_log_sent(self, operation: LocalLifecycleOperation, *, final: bool) -> None: ...
    def lifecycle_log_failed(self, operation: LocalLifecycleOperation, exc: Exception, *, phase: str) -> None: ...
    def lifecycle_complete(self, operation: LocalLifecycleOperation, *, user_delivery: str) -> None: ...


@dataclass(eq=False)
class LocalLifecycleOperation:
    access_point: AccessPointKey
    operation: str
    thread_name: str = ""
    runtime_thread_id: str = ""
    agent_id: str = ""
    cwd: str = ""
    local_command: str = ""
    notify_steward: bool = False
    resume_existing: bool = False
    show_session_identity: bool = False
    ack_message_ref: AccessPointMessageRef | None = None
    final_text: str | None = None
    outcome: LocalCommandOutcome | None = None
    startup_result: InteractiveStartupResult | None = None
    startup_persisted: bool = True
    delivery_failed: bool = False
    edit_queued: bool = False
    edit_text: str | None = None
    edit_queue_token: int | None = None
    queue_token: int | None = None
    final_sent_as_initial: bool = False
    superseded_text: str | None = None
    finished: bool = False

    def begin(self, runtime: LocalLifecycleRuntime, text: str) -> None:
        self.queue_token = runtime.lifecycle_queue_initial(self, text)

    def notice_sent(self, runtime: LocalLifecycleRuntime, message_ref: AccessPointMessageRef) -> None:
        if self.finished:
            return
        if message_ref.access_point != self.access_point:
            runtime.lifecycle_log_failed(
                self, RuntimeError("startup lifecycle notice returned a reference for another access point"),
                phase="send",
            )
            self.delivery_failed = True
        else:
            self.ack_message_ref = message_ref
            runtime.lifecycle_log_sent(self, final=False)
        self._advance(runtime)

    def notice_failed(self, runtime: LocalLifecycleRuntime, exc: Exception) -> None:
        if self.finished:
            return
        runtime.lifecycle_log_failed(self, exc, phase="send")
        self.delivery_failed = True
        self._advance(runtime)

    def notice_edited(self, runtime: LocalLifecycleRuntime, text: str) -> None:
        if self.finished:
            return
        self.edit_queued = False
        self.edit_text = None
        self.edit_queue_token = None
        if text != self.final_text:
            self._advance(runtime)
            return
        runtime.lifecycle_log_sent(self, final=True)
        self._complete(runtime, user_delivery="sent")

    def edit_failed(self, runtime: LocalLifecycleRuntime, exc: Exception, text: str) -> None:
        if self.finished:
            return
        self.edit_queued = False
        self.edit_text = None
        self.edit_queue_token = None
        if text != self.final_text:
            self._advance(runtime)
            return
        runtime.lifecycle_log_failed(self, exc, phase="edit")
        self._complete(runtime, user_delivery="failed")

    def apply_startup_result(
        self, runtime: LocalLifecycleRuntime, result: InteractiveStartupResult,
        *, persisted: bool, final_text: str, outcome: LocalCommandOutcome | None = None,
    ) -> None:
        if self.finished:
            return
        self.thread_name = result.thread_name or self.thread_name
        self.runtime_thread_id = result.thread_id or self.runtime_thread_id
        self.agent_id = result.agent_id or self.agent_id
        self.cwd = result.cwd or self.cwd
        self.startup_result = result if self.operation in {"START_AGENT", "RESUME_AGENT"} else None
        self.startup_persisted = persisted
        self.outcome = outcome
        self.final_text = final_text
        self._advance(runtime)

    def replace_with_command(
        self, runtime: LocalLifecycleRuntime, *, operation: str,
        outcome: LocalCommandOutcome, text: str,
    ) -> None:
        if self.finished:
            return
        self.operation = operation
        self.outcome = outcome
        self.final_text = text
        self._advance(runtime)

    def _advance(self, runtime: LocalLifecycleRuntime) -> None:
        if self.finished or self.final_text is None:
            return
        if self.delivery_failed:
            self._complete(runtime, user_delivery="failed")
            return
        if self.ack_message_ref is None:
            if isinstance(self.queue_token, int) and self.superseded_text != self.final_text:
                if runtime.lifecycle_supersede(self):
                    self.final_sent_as_initial = True
                    self.superseded_text = self.final_text
            return
        if self.final_sent_as_initial and self.superseded_text == self.final_text:
            runtime.lifecycle_log_sent(self, final=True)
            self._complete(runtime, user_delivery="sent")
            return
        if self.edit_queued:
            if (self.edit_text != self.final_text and isinstance(self.edit_queue_token, int)
                    and runtime.lifecycle_supersede_edit(self)):
                self.edit_text = self.final_text
            return
        self.edit_queued = True
        self.edit_text = self.final_text
        self.edit_queue_token = runtime.lifecycle_queue_edit(self)

    def _complete(self, runtime: LocalLifecycleRuntime, *, user_delivery: str) -> None:
        if self.finished:
            return
        self.finished = True
        runtime.lifecycle_complete(self, user_delivery=user_delivery)

    def tick(self, context: InteractionTickContext) -> str | None:
        return "completed" if self.finished else None

    def on_completed(self, context: InteractionTickContext, outcome: str) -> None:
        if outcome != "completed":
            raise AssertionError(f"unexpected local lifecycle outcome: {outcome}")
