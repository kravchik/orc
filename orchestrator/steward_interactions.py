"""One Steward question-to-answer cycle, including its action follow-ups."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Callable, Protocol

from orchestrator.access_point_common import AccessPointKey
from orchestrator.approval import ApprovalRequest
from orchestrator.interactive_driver_events import InteractiveAgentResult, InteractiveStartupResult
from orchestrator.interaction_queue import InteractionTickContext
from orchestrator.steward_actions import (
    build_action_loop_terminal_prompt,
    build_action_result_prompt,
    build_steward_action_fingerprint,
    parse_steward_response,
)
from orchestrator.steward_runtime_support import PendingInterruptRequest


class StewardConversationRuntime(Protocol):
    def steward_event(self, conversation: StewardConversation, name: str, **fields: Any) -> None: ...
    def steward_execute_actions(
        self, conversation: StewardConversation, actions: list[dict[str, Any]],
    ) -> list[dict[str, Any]]: ...
    def steward_begin_startup_notice(
        self, conversation: StewardConversation, results: list[dict[str, Any]],
    ) -> None: ...
    def steward_submit_followup(self, conversation: StewardConversation, prompt: str) -> None: ...
    def steward_queue_reply(self, conversation: StewardConversation, text: str) -> None: ...


@dataclass(eq=False)
class StewardConversation:
    access_point: AccessPointKey
    phase: str = "initial"
    fallback_reply: str = ""
    allow_steer: bool = False
    pending_action_results: list[dict[str, Any]] | None = None
    action_round: int = 0
    action_limit: int = 4
    last_action_fingerprint: str = ""
    action_progress: list[dict[str, Any]] = field(default_factory=list)
    action_terminal_reason: str = ""
    interrupt_notice: PendingInterruptRequest | None = None
    pending_approval_request: ApprovalRequest | None = None
    finished: bool = False

    @property
    def source(self) -> str:
        return "steward"

    def handle_result(self, runtime: StewardConversationRuntime, result: InteractiveAgentResult) -> None:
        plain_reply, actions = parse_steward_response(result.reply)
        if self.phase == "terminal_followup":
            if actions:
                runtime.steward_event(
                    self, "steward_action_loop_protocol_violation",
                    reason="action_after_terminal", completed_rounds=self.action_round,
                    action_limit=self.action_limit, action_count=len(actions),
                    action_types=self.action_types(actions),
                )
                final_reply = self.format_action_loop_fallback()
                finish_reason = "terminal_protocol_violation"
            else:
                final_reply = plain_reply
                finish_reason = "terminal_reply"
            self._finish_loop(runtime, finish_reason)
            self._queue_reply(runtime, final_reply or self.format_action_loop_fallback())
            return
        if not actions:
            final_reply = plain_reply or self.fallback_reply or "Control-plane actions completed."
            if self.action_round:
                self._finish_loop(runtime, "final_reply")
            self._queue_reply(runtime, final_reply)
            return
        if plain_reply:
            self.fallback_reply = plain_reply
        next_round = self.action_round + 1
        runtime.steward_event(
            self, "steward_actions_detected",
            action_round=next_round, action_limit=self.action_limit,
            action_count=len(actions), action_types=self.action_types(actions),
        )
        if self.action_round >= self.action_limit:
            self._terminate_loop(runtime, "round_limit", actions)
            return
        fingerprint = build_steward_action_fingerprint(actions)
        if self.last_action_fingerprint == fingerprint:
            self._terminate_loop(runtime, "duplicate_action", actions)
            return
        self.action_round = next_round
        self.last_action_fingerprint = fingerprint
        results = runtime.steward_execute_actions(self, actions)
        if any(
            str(item.get("type") or "").strip().upper() in {"START_AGENT", "RESUME_AGENT"}
            and bool(item.get("ok")) for item in results
        ):
            self.phase = "waiting_agent_startup"
            self.pending_action_results = results
            runtime.steward_begin_startup_notice(self, results)
            return
        self._submit_followup(runtime, results)

    def complete_startup(
        self, runtime: StewardConversationRuntime, result: InteractiveStartupResult, persisted: bool,
    ) -> None:
        results = self.pending_action_results
        if self.finished or self.phase != "waiting_agent_startup" or results is None:
            return
        for item in results:
            kind = str(item.get("type") or "").strip().upper()
            if kind not in {"START_AGENT", "RESUME_AGENT"} or not bool(item.get("ok")):
                continue
            item.update({
                "agent_id": result.agent_id, "state": result.state,
                "thread_id": result.thread_id, "thread_name": result.thread_name,
                "configured_model": result.configured_model, "effective_model": result.effective_model,
            })
            if result.result != "completed":
                item["ok"] = False
                item["error"] = result.error or "startup failed"
            elif not persisted:
                item["ok"] = False
                item["applied_in_memory"] = True
                item["error"] = (
                    "registry persistence failed; the running agent is active only in memory "
                    "and will be lost on restart"
                )
        self.pending_action_results = None
        self._submit_followup(runtime, results)

    def _submit_followup(self, runtime: StewardConversationRuntime, results: list[dict[str, Any]]) -> None:
        action_types = self.action_types(results)
        ok_count = sum(bool(item.get("ok")) for item in results)
        progress_actions: list[dict[str, Any]] = []
        result_codes: list[str] = []
        for item in results:
            summary: dict[str, Any] = {
                "type": str(item.get("type") or "UNKNOWN").strip().upper(),
                "ok": bool(item.get("ok")),
            }
            code = str(item.get("code") or "").strip()
            if code:
                summary["code"] = code
                result_codes.append(code)
            progress_actions.append(summary)
        self.action_progress.append({"round": self.action_round, "actions": progress_actions})
        runtime.steward_event(
            self, "steward_actions_executed", action_round=self.action_round,
            action_limit=self.action_limit, action_count=len(results),
            action_types=action_types, ok_count=ok_count,
            failed_count=len(results) - ok_count, result_codes=result_codes,
        )
        self.phase = "action_followup"
        runtime.steward_submit_followup(self, build_action_result_prompt(results))

    def _terminate_loop(
        self, runtime: StewardConversationRuntime, reason: str, pending_actions: list[dict[str, Any]],
    ) -> None:
        self.phase = "terminal_followup"
        self.action_terminal_reason = reason
        action_types = self.action_types(pending_actions)
        rejection_code = (
            "action_loop_round_limit" if reason == "round_limit" else "action_loop_duplicate_action"
        )
        rejections = [
            {
                "type": action_type, "ok": False, "code": rejection_code,
                "error": "action was not executed because the Steward action loop stopped",
            }
            for action_type in action_types
        ]
        runtime.steward_event(
            self, "steward_action_loop_terminated", reason=reason,
            completed_rounds=self.action_round, action_limit=self.action_limit,
            pending_action_count=len(pending_actions), pending_action_types=action_types,
        )
        runtime.steward_submit_followup(
            self, build_action_loop_terminal_prompt(
                reason=reason, limit=self.action_limit, completed_rounds=self.action_round,
                progress=self.action_progress, pending_actions=pending_actions,
                action_rejections=rejections,
            ),
        )

    def _finish_loop(self, runtime: StewardConversationRuntime, reason: str) -> None:
        runtime.steward_event(
            self, "steward_action_loop_finished", reason=reason,
            completed_rounds=self.action_round, action_limit=self.action_limit,
            terminal_reason=self.action_terminal_reason or None,
        )

    def _queue_reply(self, runtime: StewardConversationRuntime, text: str) -> None:
        self.phase = "waiting_reply_delivery"
        runtime.steward_queue_reply(self, text)

    @staticmethod
    def action_types(actions: list[dict[str, Any]]) -> list[str]:
        return [str(item.get("type") or "UNKNOWN").strip().upper() for item in actions]

    def format_action_loop_fallback(self) -> str:
        progress_parts: list[str] = []
        for round_summary in self.action_progress:
            round_number = int(round_summary.get("round") or 0)
            actions = round_summary.get("actions")
            if not isinstance(actions, list):
                continue
            labels = [
                f"{str(item.get('type') or 'UNKNOWN')} ({'ok' if bool(item.get('ok')) else 'failed'})"
                for item in actions if isinstance(item, dict)
            ]
            if labels:
                progress_parts.append(f"{round_number}: {', '.join(labels)}")
        lines = [
            f"Control-plane action loop stopped ({self.action_terminal_reason or 'terminal guard'}).",
            f"Completed rounds: {self.action_round}/{self.action_limit}.",
        ]
        if progress_parts:
            lines.append(f"Progress: {'; '.join(progress_parts)}.")
        lines.extend(["Pending actions were not executed.", "Continue?"])
        return "\n".join(lines)

    def steer(self, text: str, submit: Callable[[AccessPointKey, str], None]) -> bool:
        if self.finished or self.phase != "initial" or not self.allow_steer:
            return False
        submit(self.access_point, text)
        return True

    def finish(self) -> None:
        self.finished = True
        self.pending_approval_request = None

    def tick(self, context: InteractionTickContext) -> str | None:
        if self.finished and self.interrupt_notice is None:
            return "completed"
        return None

    def on_completed(self, context: InteractionTickContext, outcome: str) -> None:
        if outcome != "completed":
            raise AssertionError(f"unexpected Steward conversation outcome: {outcome}")
