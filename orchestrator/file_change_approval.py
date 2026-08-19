"""Helpers for rendering file-change approval requests."""

from __future__ import annotations

import posixpath
from typing import Any


_PATH_FIELDS = ("path", "targetPath", "newPath", "oldPath")


def enrich_file_change_approval_params_from_item(
    *,
    params: dict[str, Any],
    item: dict[str, Any] | None,
) -> dict[str, Any]:
    """Add compact file-change metadata from a tracked protocol item."""

    out = dict(params)
    existing_changes = out.get("changes")
    if isinstance(existing_changes, list) and existing_changes:
        return out
    if not isinstance(item, dict):
        return out
    if _normalize_type(item.get("type")) != "fileChange":
        return out
    raw_changes = item.get("changes")
    if not isinstance(raw_changes, list) or not raw_changes:
        return out
    changes: list[dict[str, Any]] = []
    for raw_change in raw_changes:
        if not isinstance(raw_change, dict):
            continue
        compact: dict[str, Any] = {}
        path = _extract_change_path(raw_change)
        if path:
            compact["path"] = path
        kind = _extract_change_kind(raw_change)
        if kind:
            compact["kind"] = {"type": kind}
        if compact:
            changes.append(compact)
    if changes:
        out["changes"] = changes
    return out


def format_file_change_prompt_lines(params: dict[str, Any], *, max_changes: int = 10) -> list[str]:
    raw_changes = params.get("changes")
    if not isinstance(raw_changes, list) or not raw_changes:
        return []
    rendered_changes: list[tuple[str, str]] = []
    for raw_change in raw_changes:
        if not isinstance(raw_change, dict):
            continue
        kind = _extract_change_kind(raw_change) or "change"
        path = _extract_change_path(raw_change)
        if not path:
            continue
        rendered_changes.append((kind, _basename(path)))
    if not rendered_changes:
        return []
    count = len(rendered_changes)
    suffix = "file" if count == 1 else "files"
    lines = [f"file changes: {count} {suffix}"]
    for kind, path in rendered_changes[: max(1, int(max_changes))]:
        lines.append(f"{kind}: {path}")
    remaining = count - max(1, int(max_changes))
    if remaining > 0:
        lines.append(f"... and {remaining} more")
    return lines


def extract_normalized_file_change_paths(params: dict[str, Any]) -> tuple[str, ...]:
    paths: list[str] = []
    raw_changes = params.get("changes")
    if isinstance(raw_changes, list):
        for raw_change in raw_changes:
            if not isinstance(raw_change, dict):
                continue
            for field in _PATH_FIELDS:
                _append_normalized_path(paths, raw_change.get(field))
    for field in _PATH_FIELDS:
        _append_normalized_path(paths, params.get(field))
    return tuple(dict.fromkeys(paths))


def _append_normalized_path(paths: list[str], raw_path: Any) -> None:
    if not isinstance(raw_path, str) or not raw_path.strip():
        return
    normalized = posixpath.normpath(raw_path.strip().replace("\\", "/"))
    if normalized:
        paths.append(normalized)


def _extract_change_kind(change: dict[str, Any]) -> str:
    raw_kind = change.get("kind")
    if isinstance(raw_kind, dict):
        for key in ("type", "kind", "name"):
            value = raw_kind.get(key)
            if isinstance(value, str) and value.strip():
                return value.strip()
    if isinstance(raw_kind, str) and raw_kind.strip():
        return raw_kind.strip()
    for key in ("type", "operation", "op"):
        value = change.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()
    return ""


def _extract_change_path(change: dict[str, Any]) -> str:
    for key in _PATH_FIELDS:
        value = change.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()
    return ""


def _basename(path: str) -> str:
    normalized = path.strip().replace("\\", "/").rstrip("/")
    if not normalized:
        return path.strip()
    return normalized.rsplit("/", 1)[-1] or normalized


def _normalize_type(value: Any) -> str:
    if not isinstance(value, str):
        return ""
    stripped = value.strip()
    if not stripped:
        return ""
    return stripped[:1].lower() + stripped[1:]
