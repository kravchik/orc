"""Shared helpers for thread start/resume flows."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Callable

from orchestrator.codex_sessions import find_latest_codex_cli_session_id
from orchestrator.processes import LifecycleLogger


RequestFn = Callable[[str, dict], dict]


@dataclass(frozen=True)
class ThreadStartResumeResult:
    thread_id: str
    model: str


def resolve_resume_thread_id(
    *,
    resume: bool,
    project_cwd: str,
    sessions_root: str | None,
) -> str | None:
    if not resume:
        return None
    thread_id = find_latest_codex_cli_session_id(
        project_cwd=project_cwd,
        sessions_root=sessions_root,
    )
    if not thread_id:
        raise RuntimeError(
            "resume requested but no Codex CLI session found in ~/.codex/sessions for current cwd"
        )
    return thread_id


def request_thread_start_or_resume(
    *,
    request_fn: RequestFn,
    logger: LifecycleLogger,
    role_label: str,
    start_params: dict,
    resume_thread_id: str | None,
) -> ThreadStartResumeResult:
    if resume_thread_id:
        resume_error: Exception | None = None
        for resume_params in ({"threadId": resume_thread_id}, {"id": resume_thread_id}):
            try:
                resumed = request_fn("thread/resume", resume_params)
                result = resumed.get("result") or {}
                thread_id = str((result.get("thread") or {}).get("id") or "")
                model = str(result.get("model") or "").strip()
                if not thread_id:
                    raise RuntimeError("thread/resume response did not contain thread id")
                if not model:
                    raise RuntimeError("thread/resume response did not contain model")
                logger.event(
                    f"{role_label}_thread_resumed",
                    thread_id=thread_id,
                    requested_thread_id=resume_thread_id,
                    model=model,
                )
                return ThreadStartResumeResult(thread_id=thread_id, model=model)
            except Exception as exc:
                resume_error = exc
        raise RuntimeError(
            f"thread/resume failed for requested thread id={resume_thread_id}"
        ) from resume_error

    started = request_fn("thread/start", start_params)
    result = started.get("result") or {}
    thread_id = str((result.get("thread") or {}).get("id") or "")
    model = str(result.get("model") or "").strip()
    if not thread_id:
        raise RuntimeError("thread/start response did not contain thread id")
    if not model:
        raise RuntimeError("thread/start response did not contain model")
    logger.event(f"{role_label}_thread_started", thread_id=thread_id, model=model)
    return ThreadStartResumeResult(thread_id=thread_id, model=model)
