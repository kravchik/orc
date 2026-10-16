"""Helpers for parsing and executing Steward control-plane actions."""

from __future__ import annotations

from dataclasses import dataclass
import json
import re
from pathlib import Path
from typing import Any, Callable

from orchestrator.agent_identity import ProviderKind, ProviderSessionRef
from orchestrator.agent_catalog import AgentCatalogListing, agent_catalog_failure_payloads
from orchestrator.agent_provider import ProviderSessionCatalogEntry
from orchestrator.runtime_profile_catalog import (
    BUILTIN_RUNTIME_PROFILE_CATALOG,
    RuntimeProfileCatalog,
)


_FENCED_JSON_RE = re.compile(r"```(?:json)?\s*(\{[\s\S]*?\})\s*```", re.IGNORECASE)


@dataclass(frozen=True)
class ListResumableActionRequest:
    cwd: str
    limit: int


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
    show_running_provider: Callable[[], dict[str, Any]] | None = None,
    show_running_access_point: dict[str, Any] | None = None,
    stop_agent_provider: Callable[[str], dict[str, Any]] | None = None,
    start_agent_provider: Callable[[dict[str, Any]], dict[str, Any]] | None = None,
    start_agent_access_point: dict[str, Any] | None = None,
    shaman_assign_provider: Callable[[str], dict[str, Any]] | None = None,
    shaman_show_provider: Callable[[], dict[str, Any]] | None = None,
    shaman_rename_provider: Callable[[str], dict[str, Any]] | None = None,
    shaman_remove_provider: Callable[[], dict[str, Any]] | None = None,
    grunt_assign_provider: Callable[[str], dict[str, Any]] | None = None,
    grunt_show_provider: Callable[[], dict[str, Any]] | None = None,
    grunt_rename_provider: Callable[[str], dict[str, Any]] | None = None,
    grunt_remove_provider: Callable[[], dict[str, Any]] | None = None,
    approval_target_assign_provider: Callable[[str], dict[str, Any]] | None = None,
    approval_target_show_provider: Callable[[], dict[str, Any]] | None = None,
    set_project_cwd_provider: Callable[[str], dict[str, Any]] | None = None,
    profile_catalog: RuntimeProfileCatalog = BUILTIN_RUNTIME_PROFILE_CATALOG,
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
                    profile_catalog=profile_catalog,
                )
            )
            continue
        if action_type == "STOP_AGENT":
            results.append(
                _run_stop_agent(
                    action,
                    provider=stop_agent_provider,
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
            raise RuntimeError(
                "LIST_RESUMABLE requires the asynchronous Steward action interaction"
            )
        if action_type == "SET_PROJECT_CWD":
            results.append(
                _run_set_project_cwd(
                    action,
                    provider=set_project_cwd_provider,
                    access_point=start_agent_access_point,
                )
            )
            continue
        if action_type == "SHAMAN_ASSIGN":
            results.append(
                _run_address_with_value(
                    action,
                    action_type=action_type,
                    provider=shaman_assign_provider,
                    access_point=start_agent_access_point,
                )
            )
            continue
        if action_type == "SHAMAN_SHOW":
            results.append(
                _run_address_without_value(
                    action,
                    action_type=action_type,
                    provider=shaman_show_provider,
                    access_point=start_agent_access_point,
                )
            )
            continue
        if action_type == "SHAMAN_RENAME":
            results.append(
                _run_address_with_value(
                    action,
                    action_type=action_type,
                    provider=shaman_rename_provider,
                    access_point=start_agent_access_point,
                )
            )
            continue
        if action_type == "SHAMAN_REMOVE":
            results.append(
                _run_address_without_value(
                    action,
                    action_type=action_type,
                    provider=shaman_remove_provider,
                    access_point=start_agent_access_point,
                )
            )
            continue
        if action_type == "GRUNT_ASSIGN":
            results.append(
                _run_address_with_value(
                    action,
                    action_type=action_type,
                    provider=grunt_assign_provider,
                    access_point=start_agent_access_point,
                )
            )
            continue
        if action_type == "GRUNT_SHOW":
            results.append(
                _run_address_without_value(
                    action,
                    action_type=action_type,
                    provider=grunt_show_provider,
                    access_point=start_agent_access_point,
                )
            )
            continue
        if action_type == "GRUNT_RENAME":
            results.append(
                _run_address_with_value(
                    action,
                    action_type=action_type,
                    provider=grunt_rename_provider,
                    access_point=start_agent_access_point,
                )
            )
            continue
        if action_type == "GRUNT_REMOVE":
            results.append(
                _run_address_without_value(
                    action,
                    action_type=action_type,
                    provider=grunt_remove_provider,
                    access_point=start_agent_access_point,
                )
            )
            continue
        if action_type in {
            "APPROVAL_TARGET_ASSIGN",
            "APPROVAL_TARGET_SHOW",
        }:
            results.append(
                execute_approval_target_action(
                    action,
                    access_point=start_agent_access_point,
                    assign_provider=approval_target_assign_provider,
                    show_provider=approval_target_show_provider,
                )
            )
            continue
        results.append(
            {
                "type": action_type or "UNKNOWN",
                "ok": False,
                "code": "unsupported_action",
                "error": f"unsupported action type: {action_type or 'UNKNOWN'}",
            }
        )
    return results


def _run_set_project_cwd(
    action: dict[str, Any],
    *,
    provider: Callable[[str], dict[str, Any]] | None,
    access_point: dict[str, Any] | None,
) -> dict[str, Any]:
    action_type = "SET_PROJECT_CWD"
    extra_fields = sorted(str(key) for key in action if str(key) not in {"type", "cwd"})
    if extra_fields:
        return {
            "type": action_type,
            "ok": False,
            "code": "invalid_fields",
            "error": f"SET_PROJECT_CWD has unknown fields: {', '.join(extra_fields)}",
        }
    if provider is None:
        return {
            "type": action_type,
            "ok": False,
            "code": "unavailable",
            "error": "SET_PROJECT_CWD is not available in this runtime",
        }
    cwd_raw = action.get("cwd")
    if not isinstance(cwd_raw, str) or not cwd_raw.strip():
        return {
            "type": action_type,
            "ok": False,
            "code": "invalid_cwd",
            "error": "cwd is required",
        }
    cwd_path = Path(cwd_raw.strip()).expanduser()
    try:
        resolved_cwd = cwd_path.resolve(strict=True)
    except FileNotFoundError:
        return {
            "type": action_type,
            "ok": False,
            "code": "missing_cwd",
            "error": "cwd does not exist",
        }
    except OSError:
        return {
            "type": action_type,
            "ok": False,
            "code": "invalid_cwd",
            "error": "cwd path is invalid",
        }
    if not resolved_cwd.is_dir():
        return {
            "type": action_type,
            "ok": False,
            "code": "not_directory",
            "error": "cwd must be a directory",
        }
    try:
        provider_result = provider(str(resolved_cwd))
    except Exception as exc:
        return {
            "type": action_type,
            "ok": False,
            "code": "rejected",
            "cwd": str(resolved_cwd),
            "error": str(exc).strip() or type(exc).__name__,
        }
    if not isinstance(provider_result, dict):
        raise RuntimeError("SET_PROJECT_CWD provider returned an invalid result")
    result = {
        "type": action_type,
        "cwd": str(resolved_cwd),
        **provider_result,
    }
    result["type"] = action_type
    result["cwd"] = str(resolved_cwd)
    if isinstance(access_point, dict):
        result["access_point"] = dict(access_point)
    return result


def execute_approval_target_action(
    action: dict[str, Any],
    *,
    access_point: dict[str, Any] | None = None,
    assign_provider: Callable[[str], dict[str, Any]] | None = None,
    show_provider: Callable[[], dict[str, Any]] | None = None,
) -> dict[str, Any]:
    action_type = str(action.get("type") or "").strip().upper()
    if action_type == "APPROVAL_TARGET_ASSIGN":
        return _run_approval_target_with_value(
            action,
            action_type=action_type,
            provider=assign_provider,
            access_point=access_point,
        )
    if action_type == "APPROVAL_TARGET_SHOW":
        return _run_address_without_value(
            action,
            action_type=action_type,
            provider=show_provider,
            access_point=access_point,
        )
    return {
        "type": action_type or "UNKNOWN",
        "ok": False,
        "code": "unsupported_action",
        "error": f"unsupported approval target action type: {action_type or 'UNKNOWN'}",
    }


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


def build_steward_action_fingerprint(actions: list[dict[str, Any]]) -> str:
    normalized: list[dict[str, Any]] = []
    for action in actions:
        item = dict(action)
        item["type"] = str(item.get("type") or "").strip().upper()
        normalized.append(item)
    return json.dumps(normalized, ensure_ascii=True, sort_keys=True, separators=(",", ":"))


def build_action_loop_terminal_prompt(
    *,
    reason: str,
    limit: int,
    completed_rounds: int,
    progress: list[dict[str, Any]],
    pending_actions: list[dict[str, Any]],
    action_rejections: list[dict[str, Any]],
) -> str:
    payload = {
        "action_loop_terminal": {
            "reason": reason,
            "limit": limit,
            "completed_rounds": completed_rounds,
            "progress": list(progress),
            "pending_actions": list(pending_actions),
            "action_rejections": list(action_rejections),
        }
    }
    formatted = json.dumps(payload, ensure_ascii=True, indent=2)
    return (
        "Control-plane action loop stopped before executing the pending actions. "
        "Do not return actions in this turn. Answer the user in natural language, "
        "summarize completed progress, and ask whether to continue.\n"
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


def prepare_list_resumable_action(
    action: dict[str, Any],
) -> ListResumableActionRequest | dict[str, Any]:
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
    return ListResumableActionRequest(str(resolved_cwd), limit)


def build_list_resumable_action_result(
    request: ListResumableActionRequest,
    listing: AgentCatalogListing,
    *,
    project_session_ref: Callable[[ProviderSessionRef], str],
) -> dict[str, Any]:
    if not isinstance(request, ListResumableActionRequest):
        raise TypeError("LIST_RESUMABLE result requires a prepared request")
    if not isinstance(listing, AgentCatalogListing):
        raise RuntimeError("provider session catalog returned an invalid aggregate")
    items = [
        _build_resumable_item(item, project_session_ref=project_session_ref)
        for item in listing.entries
    ]
    return {
        "type": "LIST_RESUMABLE",
        "ok": True,
        "code": "listed_partial" if listing.failures else "listed",
        "cwd": request.cwd,
        "count": len(items),
        "items": items,
        "catalog_failures": agent_catalog_failure_payloads(listing.failures),
    }


def _build_resumable_item(
    item: ProviderSessionCatalogEntry,
    *,
    project_session_ref: Callable[[ProviderSessionRef], str],
) -> dict[str, Any]:
    return {
        "thread_name": item.name,
        "timestamp": item.updated_at,
        "id": project_session_ref(item.session_ref),
        "provider": item.session_ref.provider.value,
        "profile": item.session_ref.profile.value,
    }


def _run_show_running(
    action: dict[str, Any],
    *,
    provider: Callable[[], dict[str, Any]] | None,
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
    payload = provider()
    raw_runtimes = payload.get("runtimes") if isinstance(payload, dict) else None
    runtimes = [dict(item) for item in raw_runtimes or [] if isinstance(item, dict)]
    raw_binding = payload.get("binding") if isinstance(payload, dict) else None
    binding = dict(raw_binding) if isinstance(raw_binding, dict) else None
    result: dict[str, Any] = {
        "type": "SHOW_RUNNING",
        "ok": True,
        "count": len(runtimes),
        "runtimes": runtimes,
        "binding": binding,
    }
    if isinstance(access_point, dict):
        result["access_point"] = dict(access_point)
    return result

def _run_stop_agent(
    action: dict[str, Any],
    *,
    provider: Callable[[str], dict[str, Any]] | None,
    access_point: dict[str, Any] | None,
) -> dict[str, Any]:
    extra_fields = sorted(str(key) for key in action.keys() if str(key) not in {"type", "agent_id"})
    if extra_fields:
        return {
            "type": "STOP_AGENT",
            "ok": False,
            "error": f"STOP_AGENT has unknown fields: {', '.join(extra_fields)}",
        }
    agent_id_raw = action.get("agent_id")
    if not isinstance(agent_id_raw, str) or not agent_id_raw.strip():
        return {"type": "STOP_AGENT", "ok": False, "error": "agent_id is required"}
    agent_id = agent_id_raw.strip()
    if provider is None:
        return {
            "type": "STOP_AGENT",
            "ok": False,
            "agent_id": agent_id,
            "error": "STOP_AGENT is not available in this runtime",
        }
    try:
        provider_result = provider(agent_id)
    except Exception as exc:
        return {
            "type": "STOP_AGENT",
            "ok": False,
            "agent_id": agent_id,
            "error": str(exc),
        }
    result: dict[str, Any] = {
        "type": "STOP_AGENT",
        "ok": bool(provider_result.get("ok")),
        "agent_id": agent_id,
    }
    result.update(provider_result)
    if isinstance(access_point, dict):
        result["access_point"] = dict(access_point)
    return result


def _run_address_with_value(
    action: dict[str, Any],
    *,
    action_type: str,
    provider: Callable[[str], dict[str, Any]] | None,
    access_point: dict[str, Any] | None,
) -> dict[str, Any]:
    extra_fields = sorted(str(key) for key in action.keys() if str(key) not in {"type", "address"})
    if extra_fields:
        return {
            "type": action_type,
            "ok": False,
            "error": f"{action_type} has unknown fields: {', '.join(extra_fields)}",
        }
    address = action.get("address")
    if not isinstance(address, str) or not address.strip():
        return {"type": action_type, "ok": False, "error": "address is required"}
    if provider is None:
        return {"type": action_type, "ok": False, "error": f"{action_type} is not available in this runtime"}
    try:
        provider_result = provider(address)
    except Exception as exc:
        return {"type": action_type, "ok": False, "error": str(exc)}
    return _build_address_success_result(
        action_type=action_type,
        provider_result=provider_result,
        access_point=access_point,
    )


def _run_address_without_value(
    action: dict[str, Any],
    *,
    action_type: str,
    provider: Callable[[], dict[str, Any]] | None,
    access_point: dict[str, Any] | None,
) -> dict[str, Any]:
    extra_fields = sorted(str(key) for key in action.keys() if str(key) != "type")
    if extra_fields:
        return {
            "type": action_type,
            "ok": False,
            "error": f"{action_type} does not accept fields: {', '.join(extra_fields)}",
        }
    if provider is None:
        return {"type": action_type, "ok": False, "error": f"{action_type} is not available in this runtime"}
    try:
        provider_result = provider()
    except Exception as exc:
        return {"type": action_type, "ok": False, "error": str(exc)}
    return _build_address_success_result(
        action_type=action_type,
        provider_result=provider_result,
        access_point=access_point,
    )


def _run_approval_target_with_value(
    action: dict[str, Any],
    *,
    action_type: str,
    provider: Callable[[str], dict[str, Any]] | None,
    access_point: dict[str, Any] | None,
) -> dict[str, Any]:
    extra_fields = sorted(
        str(key) for key in action.keys() if str(key) not in {"type", "approval_target"}
    )
    if extra_fields:
        return {
            "type": action_type,
            "ok": False,
            "error": f"{action_type} has unknown fields: {', '.join(extra_fields)}",
        }
    target = action.get("approval_target")
    if not isinstance(target, str) or not target.strip():
        return {"type": action_type, "ok": False, "error": "approval_target is required"}
    if provider is None:
        return {"type": action_type, "ok": False, "error": f"{action_type} is not available in this runtime"}
    try:
        provider_result = provider(target)
    except Exception as exc:
        return {"type": action_type, "ok": False, "error": str(exc)}
    return _build_address_success_result(
        action_type=action_type,
        provider_result=provider_result,
        access_point=access_point,
    )


def _build_address_success_result(
    *,
    action_type: str,
    provider_result: dict[str, Any],
    access_point: dict[str, Any] | None,
) -> dict[str, Any]:
    result: dict[str, Any] = {"type": action_type, "ok": True}
    if isinstance(provider_result, dict):
        result.update(provider_result)
    if isinstance(access_point, dict):
        result["access_point"] = dict(access_point)
    return result


def _run_start_agent(
    action: dict[str, Any],
    *,
    provider: Callable[[dict[str, Any]], dict[str, Any]] | None,
    access_point: dict[str, Any] | None,
    profile_catalog: RuntimeProfileCatalog,
) -> dict[str, Any]:
    provider_raw = action.get("provider")
    if provider_raw is None:
        provider_name = "codex"
    elif isinstance(provider_raw, str) and provider_raw.strip():
        provider_name = provider_raw.strip().lower()
    else:
        provider_name = ""
    profile_raw = action.get("profile")
    profile_name = (
        profile_raw.strip().lower()
        if isinstance(profile_raw, str) and profile_raw.strip()
        else ""
    )
    provider_kind: ProviderKind | None = None
    profile_error = ""
    if provider_name:
        try:
            execution_profile = profile_catalog.resolve(
                provider_name,
                profile_name or None,
            )
            provider_kind = execution_profile.provider
            profile_name = execution_profile.profile.value
        except ValueError as exc:
            profile_error = str(exc)

    def rejected(error: str) -> dict[str, Any]:
        result = {
            "type": "START_AGENT",
            "ok": False,
            "provider": provider_name,
            "error": error,
        }
        if profile_name:
            result["profile"] = profile_name
        return result

    allowed_fields = {
        "type",
        "provider",
        "profile",
        "cwd",
        "model",
        "mode",
        "approval_policy",
        "sandbox",
        "args",
    }
    extra_fields = sorted(str(key) for key in action.keys() if str(key) not in allowed_fields)
    if extra_fields:
        return rejected(f"START_AGENT has unknown fields: {', '.join(extra_fields)}")
    if provider is None:
        return rejected("START_AGENT is not available in this runtime")
    cwd_raw = action.get("cwd")
    if not isinstance(cwd_raw, str) or not cwd_raw.strip():
        return rejected("cwd is required")
    cwd_path = Path(cwd_raw.strip()).expanduser()
    try:
        resolved_cwd = cwd_path.resolve()
    except OSError:
        return rejected("cwd path is invalid")
    if not resolved_cwd.exists():
        return rejected("cwd does not exist")
    if not resolved_cwd.is_dir():
        return rejected("cwd must be a directory")

    if not provider_name:
        return rejected("provider must be a non-empty string")
    try:
        if provider_kind is None:
            raise ValueError(profile_error or f"unknown agent provider: {provider_name}")
        if profile_error:
            raise ValueError(profile_error)
    except ValueError as exc:
        return rejected(str(exc))

    model_raw = action.get("model")
    if model_raw is None:
        model = None
    elif isinstance(model_raw, str) and model_raw.strip():
        model = model_raw.strip()
    else:
        return rejected("model must be a non-empty string")

    mode_raw = action.get("mode")
    if mode_raw is None:
        mode = "proxy"
    elif isinstance(mode_raw, str) and mode_raw.strip():
        mode = mode_raw.strip().lower()
    else:
        return rejected("mode must be a non-empty string")
    if mode not in {"proxy", "orchestrator"}:
        return rejected("mode must be one of: proxy, orchestrator")
    if mode != "proxy":
        return rejected(f"mode is not supported yet: {mode}; only proxy is supported")

    approval_policy = action.get("approval_policy")
    if approval_policy is not None and (not isinstance(approval_policy, str) or not approval_policy.strip()):
        return rejected("approval_policy must be a non-empty string")
    sandbox = action.get("sandbox")
    if sandbox is not None and (not isinstance(sandbox, str) or not sandbox.strip()):
        return rejected("sandbox must be a non-empty string")
    args_raw = action.get("args")
    args: list[str] = []
    if args_raw is not None:
        if not isinstance(args_raw, list):
            return rejected("args must be a list of strings")
        for idx, raw in enumerate(args_raw):
            if not isinstance(raw, str) or not raw.strip():
                return rejected(f"args[{idx}] must be a non-empty string")
            args.append(raw)

    spec: dict[str, Any] = {
        "provider": provider_name,
        "profile": profile_name,
        "cwd": str(resolved_cwd),
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
        return rejected(str(exc))

    result: dict[str, Any] = {
        "type": "START_AGENT",
        "ok": True,
        "provider": spec["provider"],
        "profile": spec["profile"],
        "cwd": spec["cwd"],
        "mode": spec["mode"],
    }
    if isinstance(provider_result, dict):
        result.update(provider_result)
    result["provider"] = provider_name
    result["profile"] = profile_name
    if not str(result.get("model") or "").strip():
        result.pop("model", None)
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
    if not str(result.get("thread_id") or "").strip():
        result["thread_id"] = spec["thread_id"]
    if not str(result.get("model") or "").strip():
        result.pop("model", None)
    if isinstance(access_point, dict):
        result["access_point"] = dict(access_point)
    return result
