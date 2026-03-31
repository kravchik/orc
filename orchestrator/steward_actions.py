"""Helpers for parsing and executing Steward control-plane actions."""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any, Callable

from orchestrator.codex_sessions import list_codex_cli_sessions


_FENCED_JSON_RE = re.compile(r"```(?:json)?\s*(\{[\s\S]*?\})\s*```", re.IGNORECASE)


def parse_steward_response(message: str) -> tuple[str, list[dict[str, Any]]]:
    text = str(message or "")
    payload, span = _extract_actions_payload(text)
    if payload is None:
        return text.strip(), []
    actions_raw = payload.get("actions")
    actions = [dict(item) for item in actions_raw] if isinstance(actions_raw, list) else []
    if span is None:
        return "", actions
    start, end = span
    plain = (text[:start] + text[end:]).strip()
    return plain, actions


def execute_steward_actions(
    actions: list[dict[str, Any]],
    *,
    sessions_root: str | Path | None = None,
    show_running_provider: Callable[[], list[dict[str, Any]]] | None = None,
    show_running_access_point: dict[str, Any] | None = None,
    start_agent_provider: Callable[[dict[str, Any]], dict[str, Any]] | None = None,
    start_agent_access_point: dict[str, Any] | None = None,
    list_routing_profiles_provider: Callable[[], list[dict[str, Any]]] | None = None,
    get_routing_profile_provider: Callable[[str], dict[str, Any] | None] | None = None,
    set_routing_profile_provider: Callable[[dict[str, Any]], dict[str, Any]] | None = None,
    attach_routing_profile_provider: Callable[[dict[str, Any]], dict[str, Any]] | None = None,
    detach_routing_profile_provider: Callable[[dict[str, Any]], dict[str, Any]] | None = None,
) -> list[dict[str, Any]]:
    results: list[dict[str, Any]] = []
    for raw in actions:
        action = dict(raw)
        action_type = str(action.get("type") or "").strip().upper()
        if action_type == "RESUME_AGENT":
            results.append(
                _run_resume_agent(
                    action,
                    provider=start_agent_provider,
                    access_point=start_agent_access_point,
                )
            )
            continue
        if action_type == "START_AGENT":
            results.append(
                _run_start_agent(
                    action,
                    provider=start_agent_provider,
                    access_point=start_agent_access_point,
                )
            )
            continue
        if action_type == "SHOW_RUNNING":
            results.append(
                _run_show_running(
                    action,
                    provider=show_running_provider,
                    access_point=show_running_access_point,
                )
            )
            continue
        if action_type == "LIST_RESUMABLE":
            results.append(_run_list_resumable(action, sessions_root=sessions_root))
            continue
        if action_type == "ROUTING_PROFILE_LIST":
            results.append(
                _run_routing_profile_list(
                    action,
                    provider=list_routing_profiles_provider,
                )
            )
            continue
        if action_type == "ROUTING_PROFILE_GET":
            results.append(
                _run_routing_profile_get(
                    action,
                    provider=get_routing_profile_provider,
                )
            )
            continue
        if action_type == "ROUTING_PROFILE_SET":
            results.append(
                _run_routing_profile_set(
                    action,
                    provider=set_routing_profile_provider,
                )
            )
            continue
        if action_type == "ROUTING_PROFILE_ATTACH":
            results.append(
                _run_routing_profile_attach(
                    action,
                    provider=attach_routing_profile_provider,
                )
            )
            continue
        if action_type == "ROUTING_PROFILE_DETACH":
            results.append(
                _run_routing_profile_detach(
                    action,
                    provider=detach_routing_profile_provider,
                )
            )
            continue
        results.append(
            {
                "type": action_type or "UNKNOWN",
                "ok": False,
                "error": f"unsupported action type: {action_type or 'UNKNOWN'}",
            }
        )
    return results


def build_action_result_prompt(results: list[dict[str, Any]]) -> str:
    payload = {"action_results": list(results)}
    formatted = json.dumps(payload, ensure_ascii=True, indent=2)
    return (
        "Control-plane executed your requested actions. "
        "Use action_results to answer user in natural language.\n"
        "```json\n"
        f"{formatted}\n"
        "```"
    )


def _extract_actions_payload(text: str) -> tuple[dict[str, Any] | None, tuple[int, int] | None]:
    for match in _FENCED_JSON_RE.finditer(text):
        candidate = match.group(1).strip()
        payload = _parse_actions_payload(candidate)
        if payload is not None:
            return payload, match.span()
    stripped = text.strip()
    payload = _parse_actions_payload(stripped)
    if payload is not None:
        return payload, None
    return None, None


def _parse_actions_payload(raw: str) -> dict[str, Any] | None:
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError:
        return None
    if not isinstance(payload, dict):
        return None
    actions = payload.get("actions")
    if not isinstance(actions, list):
        return None
    for item in actions:
        if not isinstance(item, dict):
            return None
        action_type = item.get("type")
        if not isinstance(action_type, str) or not action_type.strip():
            return None
    return payload


def _run_list_resumable(
    action: dict[str, Any],
    *,
    sessions_root: str | Path | None = None,
) -> dict[str, Any]:
    cwd_raw = action.get("cwd")
    if not isinstance(cwd_raw, str) or not cwd_raw.strip():
        return {"type": "LIST_RESUMABLE", "ok": False, "error": "cwd is required"}
    cwd = cwd_raw.strip()
    cwd_path = Path(cwd).expanduser()
    try:
        resolved_cwd = cwd_path.resolve()
    except OSError:
        return {"type": "LIST_RESUMABLE", "ok": False, "error": "cwd path is invalid"}
    if not resolved_cwd.exists():
        return {"type": "LIST_RESUMABLE", "ok": False, "error": "cwd does not exist"}
    if not resolved_cwd.is_dir():
        return {"type": "LIST_RESUMABLE", "ok": False, "error": "cwd must be a directory"}
    limit_raw = action.get("limit")
    limit = 20
    if isinstance(limit_raw, int):
        limit = limit_raw
    elif isinstance(limit_raw, str) and limit_raw.strip():
        try:
            limit = int(limit_raw.strip())
        except ValueError:
            return {"type": "LIST_RESUMABLE", "ok": False, "error": "limit must be integer"}
    if limit < 0:
        return {"type": "LIST_RESUMABLE", "ok": False, "error": "limit must be >= 0"}
    sessions = list_codex_cli_sessions(
        project_cwd=str(resolved_cwd),
        sessions_root=sessions_root,
        limit=limit,
    )
    items = [
        _build_resumable_item(item)
        for item in sessions
    ]
    return {
        "type": "LIST_RESUMABLE",
        "ok": True,
        "cwd": str(resolved_cwd),
        "count": len(items),
        "items": items,
    }


def _build_resumable_item(item) -> dict[str, Any]:
    return {
        "thread_name": item.thread_name or "",
        "timestamp": item.last_activity_iso,
        "id": item.session_id,
    }


def _run_show_running(
    action: dict[str, Any],
    *,
    provider: Callable[[], list[dict[str, Any]]] | None,
    access_point: dict[str, Any] | None,
) -> dict[str, Any]:
    extra_fields = sorted(str(key) for key in action.keys() if str(key) != "type")
    if extra_fields:
        return {
            "type": "SHOW_RUNNING",
            "ok": False,
            "error": f"SHOW_RUNNING does not accept fields: {', '.join(extra_fields)}",
        }
    if provider is None:
        return {
            "type": "SHOW_RUNNING",
            "ok": False,
            "error": "SHOW_RUNNING is not available in this runtime",
        }
    raw_items = provider()
    items: list[dict[str, Any]] = []
    for raw in raw_items:
        if not isinstance(raw, dict):
            continue
        agent_id = str(raw.get("agent_id") or "").strip()
        state = str(raw.get("state") or "").strip()
        if not agent_id:
            continue
        items.append({"agent_id": agent_id, "state": state or "unknown"})
    result: dict[str, Any] = {
        "type": "SHOW_RUNNING",
        "ok": True,
        "count": len(items),
        "items": items,
    }
    if isinstance(access_point, dict):
        result["access_point"] = dict(access_point)
    return result


def _run_start_agent(
    action: dict[str, Any],
    *,
    provider: Callable[[dict[str, Any]], dict[str, Any]] | None,
    access_point: dict[str, Any] | None,
) -> dict[str, Any]:
    allowed_fields = {
        "type",
        "cwd",
        "model",
        "mode",
        "approval_policy",
        "sandbox",
        "args",
    }
    extra_fields = sorted(str(key) for key in action.keys() if str(key) not in allowed_fields)
    if extra_fields:
        return {
            "type": "START_AGENT",
            "ok": False,
            "error": f"START_AGENT has unknown fields: {', '.join(extra_fields)}",
        }
    if provider is None:
        return {
            "type": "START_AGENT",
            "ok": False,
            "error": "START_AGENT is not available in this runtime",
        }
    cwd_raw = action.get("cwd")
    if not isinstance(cwd_raw, str) or not cwd_raw.strip():
        return {"type": "START_AGENT", "ok": False, "error": "cwd is required"}
    cwd_path = Path(cwd_raw.strip()).expanduser()
    try:
        resolved_cwd = cwd_path.resolve()
    except OSError:
        return {"type": "START_AGENT", "ok": False, "error": "cwd path is invalid"}
    if not resolved_cwd.exists():
        return {"type": "START_AGENT", "ok": False, "error": "cwd does not exist"}
    if not resolved_cwd.is_dir():
        return {"type": "START_AGENT", "ok": False, "error": "cwd must be a directory"}

    model_raw = action.get("model")
    if model_raw is None:
        model = None
    elif isinstance(model_raw, str) and model_raw.strip():
        model = model_raw.strip()
    else:
        return {"type": "START_AGENT", "ok": False, "error": "model must be a non-empty string"}

    mode_raw = action.get("mode")
    if mode_raw is None:
        mode = "proxy"
    elif isinstance(mode_raw, str) and mode_raw.strip():
        mode = mode_raw.strip().lower()
    else:
        return {"type": "START_AGENT", "ok": False, "error": "mode must be a non-empty string"}
    if mode not in {"proxy", "orchestrator"}:
        return {"type": "START_AGENT", "ok": False, "error": "mode must be one of: proxy, orchestrator"}
    if mode != "proxy":
        return {
            "type": "START_AGENT",
            "ok": False,
            "error": f"mode is not supported yet: {mode}; only proxy is supported",
        }

    approval_policy = action.get("approval_policy")
    if approval_policy is not None and (not isinstance(approval_policy, str) or not approval_policy.strip()):
        return {"type": "START_AGENT", "ok": False, "error": "approval_policy must be a non-empty string"}
    sandbox = action.get("sandbox")
    if sandbox is not None and (not isinstance(sandbox, str) or not sandbox.strip()):
        return {"type": "START_AGENT", "ok": False, "error": "sandbox must be a non-empty string"}
    args_raw = action.get("args")
    args: list[str] = []
    if args_raw is not None:
        if not isinstance(args_raw, list):
            return {"type": "START_AGENT", "ok": False, "error": "args must be a list of strings"}
        for idx, raw in enumerate(args_raw):
            if not isinstance(raw, str) or not raw.strip():
                return {
                    "type": "START_AGENT",
                    "ok": False,
                    "error": f"args[{idx}] must be a non-empty string",
                }
            args.append(raw)

    spec: dict[str, Any] = {"cwd": str(resolved_cwd), "mode": mode, "args": args}
    if isinstance(model, str) and model.strip():
        spec["model"] = model
    if isinstance(approval_policy, str) and approval_policy.strip():
        spec["approval_policy"] = approval_policy.strip()
    if isinstance(sandbox, str) and sandbox.strip():
        spec["sandbox"] = sandbox.strip()
    try:
        provider_result = provider(spec)
    except Exception as exc:
        return {"type": "START_AGENT", "ok": False, "error": str(exc)}

    result: dict[str, Any] = {
        "type": "START_AGENT",
        "ok": True,
        "cwd": spec["cwd"],
        "mode": spec["mode"],
    }
    if isinstance(provider_result, dict):
        result.update(provider_result)
    if isinstance(access_point, dict):
        result["access_point"] = dict(access_point)
    return result


def _run_resume_agent(
    action: dict[str, Any],
    *,
    provider: Callable[[dict[str, Any]], dict[str, Any]] | None,
    access_point: dict[str, Any] | None,
) -> dict[str, Any]:
    allowed_fields = {
        "type",
        "cwd",
        "thread_id",
        "model",
        "mode",
        "approval_policy",
        "sandbox",
        "args",
    }
    extra_fields = sorted(str(key) for key in action.keys() if str(key) not in allowed_fields)
    if extra_fields:
        return {
            "type": "RESUME_AGENT",
            "ok": False,
            "error": f"RESUME_AGENT has unknown fields: {', '.join(extra_fields)}",
        }
    if provider is None:
        return {
            "type": "RESUME_AGENT",
            "ok": False,
            "error": "RESUME_AGENT is not available in this runtime",
        }
    cwd_raw = action.get("cwd")
    if not isinstance(cwd_raw, str) or not cwd_raw.strip():
        return {"type": "RESUME_AGENT", "ok": False, "error": "cwd is required"}
    cwd_path = Path(cwd_raw.strip()).expanduser()
    try:
        resolved_cwd = cwd_path.resolve()
    except OSError:
        return {"type": "RESUME_AGENT", "ok": False, "error": "cwd path is invalid"}
    if not resolved_cwd.exists():
        return {"type": "RESUME_AGENT", "ok": False, "error": "cwd does not exist"}
    if not resolved_cwd.is_dir():
        return {"type": "RESUME_AGENT", "ok": False, "error": "cwd must be a directory"}

    thread_id_raw = action.get("thread_id")
    if not isinstance(thread_id_raw, str) or not thread_id_raw.strip():
        return {"type": "RESUME_AGENT", "ok": False, "error": "thread_id is required"}
    thread_id = thread_id_raw.strip()

    model_raw = action.get("model")
    if model_raw is None:
        model = None
    elif isinstance(model_raw, str) and model_raw.strip():
        model = model_raw.strip()
    else:
        return {"type": "RESUME_AGENT", "ok": False, "error": "model must be a non-empty string"}

    mode_raw = action.get("mode")
    if mode_raw is None:
        mode = "proxy"
    elif isinstance(mode_raw, str) and mode_raw.strip():
        mode = mode_raw.strip().lower()
    else:
        return {"type": "RESUME_AGENT", "ok": False, "error": "mode must be a non-empty string"}
    if mode not in {"proxy", "orchestrator"}:
        return {"type": "RESUME_AGENT", "ok": False, "error": "mode must be one of: proxy, orchestrator"}
    if mode != "proxy":
        return {
            "type": "RESUME_AGENT",
            "ok": False,
            "error": f"mode is not supported yet: {mode}; only proxy is supported",
        }

    approval_policy = action.get("approval_policy")
    if approval_policy is not None and (not isinstance(approval_policy, str) or not approval_policy.strip()):
        return {"type": "RESUME_AGENT", "ok": False, "error": "approval_policy must be a non-empty string"}
    sandbox = action.get("sandbox")
    if sandbox is not None and (not isinstance(sandbox, str) or not sandbox.strip()):
        return {"type": "RESUME_AGENT", "ok": False, "error": "sandbox must be a non-empty string"}
    args_raw = action.get("args")
    args: list[str] = []
    if args_raw is not None:
        if not isinstance(args_raw, list):
            return {"type": "RESUME_AGENT", "ok": False, "error": "args must be a list of strings"}
        for idx, raw in enumerate(args_raw):
            if not isinstance(raw, str) or not raw.strip():
                return {
                    "type": "RESUME_AGENT",
                    "ok": False,
                    "error": f"args[{idx}] must be a non-empty string",
                }
            args.append(raw)

    spec: dict[str, Any] = {
        "cwd": str(resolved_cwd),
        "thread_id": thread_id,
        "mode": mode,
        "args": args,
    }
    if isinstance(model, str) and model.strip():
        spec["model"] = model
    if isinstance(approval_policy, str) and approval_policy.strip():
        spec["approval_policy"] = approval_policy.strip()
    if isinstance(sandbox, str) and sandbox.strip():
        spec["sandbox"] = sandbox.strip()
    try:
        provider_result = provider(spec)
    except Exception as exc:
        return {"type": "RESUME_AGENT", "ok": False, "error": str(exc)}

    result: dict[str, Any] = {
        "type": "RESUME_AGENT",
        "ok": True,
        "cwd": spec["cwd"],
        "thread_id": spec["thread_id"],
        "mode": spec["mode"],
    }
    if isinstance(provider_result, dict):
        result.update(provider_result)
    if isinstance(access_point, dict):
        result["access_point"] = dict(access_point)
    return result


def _run_routing_profile_list(
    action: dict[str, Any],
    *,
    provider: Callable[[], list[dict[str, Any]]] | None,
) -> dict[str, Any]:
    extra_fields = sorted(str(key) for key in action.keys() if str(key) != "type")
    if extra_fields:
        return {
            "type": "ROUTING_PROFILE_LIST",
            "ok": False,
            "error": f"ROUTING_PROFILE_LIST does not accept fields: {', '.join(extra_fields)}",
        }
    if provider is None:
        return {
            "type": "ROUTING_PROFILE_LIST",
            "ok": False,
            "error": "ROUTING_PROFILE_LIST is not available in this runtime",
        }
    try:
        items_raw = provider()
    except Exception as exc:
        return {"type": "ROUTING_PROFILE_LIST", "ok": False, "error": str(exc)}
    items = [dict(item) for item in items_raw if isinstance(item, dict)]
    return {
        "type": "ROUTING_PROFILE_LIST",
        "ok": True,
        "count": len(items),
        "items": items,
    }


def _run_routing_profile_get(
    action: dict[str, Any],
    *,
    provider: Callable[[str], dict[str, Any] | None] | None,
) -> dict[str, Any]:
    allowed_fields = {"type", "session_id"}
    extra_fields = sorted(str(key) for key in action.keys() if str(key) not in allowed_fields)
    if extra_fields:
        return {
            "type": "ROUTING_PROFILE_GET",
            "ok": False,
            "error": f"ROUTING_PROFILE_GET has unknown fields: {', '.join(extra_fields)}",
        }
    if provider is None:
        return {
            "type": "ROUTING_PROFILE_GET",
            "ok": False,
            "error": "ROUTING_PROFILE_GET is not available in this runtime",
        }
    session_id = str(action.get("session_id") or "").strip()
    if not session_id:
        return {"type": "ROUTING_PROFILE_GET", "ok": False, "error": "session_id is required"}
    try:
        payload = provider(session_id)
    except Exception as exc:
        return {"type": "ROUTING_PROFILE_GET", "ok": False, "error": str(exc)}
    if payload is None:
        return {
            "type": "ROUTING_PROFILE_GET",
            "ok": False,
            "error": f"unknown session id: {session_id}",
        }
    result = {"type": "ROUTING_PROFILE_GET", "ok": True}
    result.update(dict(payload))
    return result


def _run_routing_profile_set(
    action: dict[str, Any],
    *,
    provider: Callable[[dict[str, Any]], dict[str, Any]] | None,
) -> dict[str, Any]:
    allowed_fields = {"type", "session_id", "profile", "roles"}
    extra_fields = sorted(str(key) for key in action.keys() if str(key) not in allowed_fields)
    if extra_fields:
        return {
            "type": "ROUTING_PROFILE_SET",
            "ok": False,
            "error": f"ROUTING_PROFILE_SET has unknown fields: {', '.join(extra_fields)}",
        }
    if provider is None:
        return {
            "type": "ROUTING_PROFILE_SET",
            "ok": False,
            "error": "ROUTING_PROFILE_SET is not available in this runtime",
        }
    session_id = str(action.get("session_id") or "").strip()
    if not session_id:
        return {"type": "ROUTING_PROFILE_SET", "ok": False, "error": "session_id is required"}
    profile = str(action.get("profile") or "").strip()
    if not profile:
        return {"type": "ROUTING_PROFILE_SET", "ok": False, "error": "profile is required"}
    roles_raw = action.get("roles")
    if not isinstance(roles_raw, dict) or not roles_raw:
        return {"type": "ROUTING_PROFILE_SET", "ok": False, "error": "roles is required and must be object"}
    roles: dict[str, str] = {}
    for key, value in roles_raw.items():
        alias = str(key or "").strip()
        node_id = str(value or "").strip()
        if not alias:
            return {"type": "ROUTING_PROFILE_SET", "ok": False, "error": "roles keys must be non-empty"}
        if not node_id:
            return {
                "type": "ROUTING_PROFILE_SET",
                "ok": False,
                "error": f"roles[{alias!r}] must be non-empty string",
            }
        roles[alias] = node_id
    spec = {
        "session_id": session_id,
        "profile": profile,
        "roles": roles,
    }
    try:
        payload = provider(spec)
    except Exception as exc:
        return {"type": "ROUTING_PROFILE_SET", "ok": False, "error": str(exc)}
    result = {"type": "ROUTING_PROFILE_SET", "ok": True}
    if isinstance(payload, dict):
        result.update(dict(payload))
    return result


def _run_routing_profile_attach(
    action: dict[str, Any],
    *,
    provider: Callable[[dict[str, Any]], dict[str, Any]] | None,
) -> dict[str, Any]:
    allowed_fields = {"type", "session_id", "source_alias"}
    extra_fields = sorted(str(key) for key in action.keys() if str(key) not in allowed_fields)
    if extra_fields:
        return {
            "type": "ROUTING_PROFILE_ATTACH",
            "ok": False,
            "error": f"ROUTING_PROFILE_ATTACH has unknown fields: {', '.join(extra_fields)}",
        }
    if provider is None:
        return {
            "type": "ROUTING_PROFILE_ATTACH",
            "ok": False,
            "error": "ROUTING_PROFILE_ATTACH is not available in this runtime",
        }
    session_id = str(action.get("session_id") or "").strip()
    if not session_id:
        return {"type": "ROUTING_PROFILE_ATTACH", "ok": False, "error": "session_id is required"}
    source_alias = str(action.get("source_alias") or "@HUMAN").strip()
    if not source_alias:
        return {"type": "ROUTING_PROFILE_ATTACH", "ok": False, "error": "source_alias must be non-empty"}
    spec = {
        "session_id": session_id,
        "source_alias": source_alias,
    }
    try:
        payload = provider(spec)
    except Exception as exc:
        return {"type": "ROUTING_PROFILE_ATTACH", "ok": False, "error": str(exc)}
    result = {"type": "ROUTING_PROFILE_ATTACH", "ok": True}
    if isinstance(payload, dict):
        result.update(dict(payload))
    return result


def _run_routing_profile_detach(
    action: dict[str, Any],
    *,
    provider: Callable[[dict[str, Any]], dict[str, Any]] | None,
) -> dict[str, Any]:
    allowed_fields = {"type"}
    extra_fields = sorted(str(key) for key in action.keys() if str(key) not in allowed_fields)
    if extra_fields:
        return {
            "type": "ROUTING_PROFILE_DETACH",
            "ok": False,
            "error": f"ROUTING_PROFILE_DETACH has unknown fields: {', '.join(extra_fields)}",
        }
    if provider is None:
        return {
            "type": "ROUTING_PROFILE_DETACH",
            "ok": False,
            "error": "ROUTING_PROFILE_DETACH is not available in this runtime",
        }
    try:
        payload = provider({})
    except Exception as exc:
        return {"type": "ROUTING_PROFILE_DETACH", "ok": False, "error": str(exc)}
    result = {"type": "ROUTING_PROFILE_DETACH", "ok": True}
    if isinstance(payload, dict):
        result.update(dict(payload))
    return result
