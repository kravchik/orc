from __future__ import annotations

from typing import Any, Callable, Optional

from orchestrator.adapters import CodexJsonRpcSession
from orchestrator.approval import ApprovalDecisionProvider, ApprovalPolicy
from orchestrator.processes import LifecycleLogger
from orchestrator.protocol_status import format_item_status_summary, format_protocol_status


class CodexSingleAgentBackend:
    def __init__(
        self,
        command: list[str],
        logger: LifecycleLogger,
        request_timeout_sec: float = 90.0,
        rpc_timeout_sec: float = 5.0,
        rpc_retries: int = 3,
        approval_policy: Optional[ApprovalPolicy] = None,
        approval_decision_provider: Optional[ApprovalDecisionProvider] = None,
        thread_approval_policy: str = "untrusted",
        thread_sandbox: str = "workspace-write",
        resume_thread_id: str | None = None,
        working_dir: str | None = None,
        thread_model: str | None = None,
        client_factory: Optional[Callable[..., Any]] = None,
    ) -> None:
        self._logger = logger
        self._request_timeout_sec = request_timeout_sec
        self._rpc_timeout_sec = max(0.1, float(rpc_timeout_sec))
        self._rpc_retries = max(0, int(rpc_retries))
        self._approval_policy = approval_policy
        self._approval_decision_provider = approval_decision_provider
        self._thread_approval_policy = thread_approval_policy
        self._thread_sandbox = thread_sandbox
        self._resume_thread_id = resume_thread_id
        self._working_dir = working_dir
        self._thread_model = thread_model.strip() if isinstance(thread_model, str) and thread_model.strip() else None
        self._actual_thread_model: str | None = None
        self._session = CodexJsonRpcSession(
            role="agent",
            command=command,
            logger=self._logger,
            cwd=self._working_dir,
            env=None,
            request_timeout_sec=self._request_timeout_sec,
            rpc_timeout_sec=self._rpc_timeout_sec,
            rpc_retries=self._rpc_retries,
            approval_policy=self._approval_policy,
            fsm=None,
            thread_approval_policy=self._thread_approval_policy,
            thread_sandbox=self._thread_sandbox,
            approval_decision_provider=self._approval_decision_provider,
            approval_notify_callback=None,
            protocol_status_callback=None,
            protocol_log_include_params=True,
            client_factory=client_factory,
        )
        self._protocol_status_callback: Optional[Callable[[str, dict], None]] = None
        self._event_callback = None

    def start(self) -> None:
        self._actual_thread_model = None
        start_params = {"approvalPolicy": self._thread_approval_policy, "sandbox": self._thread_sandbox}
        if self._thread_model is not None:
            start_params["model"] = self._thread_model
        try:
            self._session.start(
                resume_thread_id=self._resume_thread_id,
                start_params=start_params,
            )
        except Exception:
            if self._thread_model is None or self._resume_thread_id is not None:
                raise
            self._logger.event(
                "agent_thread_start_retry_without_model",
                model=self._thread_model,
            )
            self._session.start(
                resume_thread_id=self._resume_thread_id,
                start_params={"approvalPolicy": self._thread_approval_policy, "sandbox": self._thread_sandbox},
            )
        self._logger.event("agent_backend_started", pid=self._session.pid)
        actual_model = str(self._session.thread_model or "").strip()
        if not actual_model:
            raise RuntimeError("agent thread started but model missing in thread/start or thread/resume response")
        self._actual_thread_model = actual_model

    def ask(self, prompt: str, chat_id: int | None = None) -> str:
        _ = chat_id
        notify = None
        if (
            self._approval_decision_provider is not None
            and hasattr(self._approval_decision_provider, "notify_auto_approval")
        ):
            notify = self._approval_decision_provider.notify_auto_approval  # type: ignore[attr-defined]
        return self._session.ask(prompt, approval_notify_callback=notify)

    def close(self) -> None:
        pid = self._session.pid
        self._session.stop()
        self._logger.event("agent_backend_stopped", pid=pid, returncode=self._session.returncode)

    def stop(self) -> None:
        self.close()

    def set_protocol_status_callback(
        self,
        callback: Optional[Callable[[str, dict], None]],
    ) -> None:
        self._protocol_status_callback = callback
        if callback is None:
            self._session.set_protocol_status_callback(None)
            return
        self._session.set_protocol_status_callback(
            lambda _role, method, params: callback(method, params)
        )

    def set_event_callback(self, callback) -> None:
        self._event_callback = callback

    def get_actual_thread_model(self) -> str:
        return str(self._actual_thread_model or "")

    def apply_model(self, model: str) -> str:
        requested_model = str(model or "").strip()
        if not requested_model:
            raise ValueError("model must be a non-empty string")
        self._logger.event(
            "model_apply_requested",
            requested_model=requested_model,
        )
        self._thread_model = requested_model
        self._resume_thread_id = None
        self._session.stop()
        self.start()
        actual = self.get_actual_thread_model()
        self._logger.event(
            "model_apply_completed",
            requested_model=requested_model,
            actual_model=actual,
        )
        return actual

    def set_protocol_item_callback(
        self,
        callback: Optional[Callable[[str, str], None]],
    ) -> None:
        self._session.set_protocol_item_callback(callback)

    def get_item_status_snapshot(self) -> list[dict[str, str]]:
        return self._session.get_item_status_snapshot()

    def get_item_status_snapshot_for_turn(self, turn_id: str) -> list[dict[str, str]]:
        return self._session.get_item_status_snapshot(turn_id=turn_id)

    def get_last_item_apply_info(self) -> dict | None:
        return self._session.get_last_item_apply_info()

    def send(self, text: str, *, status_update=None, approval_notify=None) -> str:
        _ = approval_notify
        self.set_protocol_item_callback(
            (lambda _role, changed_item_id: status_update(
                format_item_status_summary(
                    self.get_item_status_snapshot(),
                    changed_item_id=changed_item_id,
                )
            ))
            if status_update is not None
            else None
        )
        self.set_protocol_status_callback(
            (lambda method, params: status_update(_format_protocol_status_for_telegram(method=method, params=params)))
            if status_update is not None
            else None
        )
        try:
            return self.ask(text)
        finally:
            self.set_protocol_item_callback(None)
            self.set_protocol_status_callback(None)


def _format_context_usage_status(method: str, params: dict | None) -> str | None:
    if method != "thread/tokenUsage/updated" or not isinstance(params, dict):
        return None
    token_usage = params.get("tokenUsage")
    if not isinstance(token_usage, dict):
        return None
    total_obj = token_usage.get("total")
    if not isinstance(total_obj, dict):
        return None
    total_tokens = total_obj.get("totalTokens")
    model_context = token_usage.get("modelContextWindow")
    if not isinstance(total_tokens, int) or not isinstance(model_context, int) or model_context <= 0:
        return None
    pct = max(0.0, min(100.0, (float(total_tokens) / float(model_context)) * 100.0))
    return f"thread/tokenUsage/updated ({pct:.1f}% ctx: {total_tokens}/{model_context})"


def _format_protocol_status_for_telegram(method: str, params: dict | None) -> str:
    usage = _format_context_usage_status(method=method, params=params)
    if usage is not None:
        return usage
    return format_protocol_status(method=method, params=params)
