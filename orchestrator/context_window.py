"""Model context-window usage derived from app-server notifications."""

from __future__ import annotations

from typing import Any


COMPACTION_METHODS = {
    "thread/compacted",
    "thread/context/compacted",
    "codex/event/context_compacted",
}


def parse_context_usage(
    params: dict[str, Any] | None,
    *,
    expected_thread_id: str,
) -> dict[str, object] | None:
    if not isinstance(params, dict):
        return None
    thread_id = _text(params.get("threadId"))
    if not thread_id or thread_id != expected_thread_id:
        return None
    token_usage = params.get("tokenUsage")
    if not isinstance(token_usage, dict):
        return None
    last = token_usage.get("last")
    if not isinstance(last, dict):
        return None
    used_tokens = _positive_or_zero_int(last.get("totalTokens"))
    model_context_window = _positive_int(token_usage.get("modelContextWindow"))
    if used_tokens is None or model_context_window is None:
        return None
    return {
        "thread_id": thread_id,
        "turn_id": _text(params.get("turnId")),
        "used_tokens": used_tokens,
        "model_context_window": model_context_window,
    }


def compaction_matches_thread(
    method: str,
    params: dict[str, Any] | None,
    *,
    expected_thread_id: str,
) -> bool:
    if method not in COMPACTION_METHODS or not expected_thread_id:
        return False
    if not isinstance(params, dict):
        return True
    thread_id = _text(params.get("threadId"))
    return not thread_id or thread_id == expected_thread_id


def format_context_window_remaining(context_usage: dict[str, object] | None) -> str:
    usage = context_usage if isinstance(context_usage, dict) else {}
    used_tokens = _positive_or_zero_int(usage.get("used_tokens"))
    model_context_window = _positive_int(usage.get("model_context_window"))
    if used_tokens is None or model_context_window is None:
        return "model window remaining: unknown"
    remaining = max(0, model_context_window - used_tokens)
    percent = max(0.0, min(100.0, remaining * 100.0 / model_context_window))
    return (
        f"model window remaining: {remaining}/{model_context_window} tokens "
        f"({percent:.1f}%)"
    )


def format_context_usage_status(method: str, params: dict[str, Any] | None) -> str | None:
    if method != "thread/tokenUsage/updated" or not isinstance(params, dict):
        return None
    thread_id = _text(params.get("threadId"))
    if not thread_id:
        return None
    usage = parse_context_usage(params, expected_thread_id=thread_id)
    if usage is None:
        return None
    return f"thread/tokenUsage/updated ({format_context_window_remaining(usage)})"


def _text(value: object) -> str:
    return value.strip() if isinstance(value, str) else ""


def _positive_or_zero_int(value: object) -> int | None:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        return None
    return value


def _positive_int(value: object) -> int | None:
    normalized = _positive_or_zero_int(value)
    if normalized is None or normalized <= 0:
        return None
    return normalized
