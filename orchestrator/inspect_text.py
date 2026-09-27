from __future__ import annotations

from datetime import datetime, timezone
from typing import Any


def format_thread_metadata_lines(
    metadata: dict[str, Any] | None,
    *,
    thread_id_fallback: str = "",
    thread_name_fallback: str = "",
    model_fallback: str = "",
    include_model: bool = True,
) -> list[str]:
    raw = dict(metadata or {})
    lines: list[str] = []

    name = _normalize_text(raw.get("name")) or thread_name_fallback.strip()
    if name:
        lines.append(f"thread name: {name}")
    else:
        thread_id = (
            _normalize_text(raw.get("id"))
            or _normalize_text(raw.get("thread_id"))
            or thread_id_fallback.strip()
        )
        if thread_id:
            lines.append(f"thread id: {thread_id}")

    status_type, active_flags = _normalize_status(raw.get("status"))
    if status_type:
        lines.append(f"thread status: {status_type}")
    if active_flags:
        lines.append(f"active flags: {', '.join(active_flags)}")

    loaded = _normalize_loaded(raw.get("loaded"))
    if loaded:
        lines.append(f"loaded: {loaded}")

    ephemeral = _normalize_bool_label(raw.get("ephemeral"))
    if ephemeral:
        lines.append(f"ephemeral: {ephemeral}")

    provider = _normalize_text(raw.get("modelProvider")) or _normalize_text(raw.get("provider"))
    if provider:
        lines.append(f"provider: {provider}")

    created_at = _format_timestamp(raw.get("createdAt"))
    if created_at:
        lines.append(f"created at: {created_at}")

    updated_at = _format_timestamp(raw.get("updatedAt"))
    if updated_at:
        lines.append(f"updated at: {updated_at}")

    preview = _normalize_text(raw.get("preview"))
    if preview:
        lines.append(f"preview: {preview}")

    if include_model:
        model = _normalize_text(raw.get("model")) or model_fallback.strip()
        if model:
            lines.append(f"model: {model}")

    return lines


def append_active_work_lines(lines: list[str], rows: list[dict[str, str]]) -> None:
    active_rows = [row for row in rows if str(row.get("status", "")).strip() == "in_progress"]
    lines.append(f"open items tracked by ORC: {len(active_rows)}")
    if not active_rows:
        lines.append("none")
        return
    for row in active_rows:
        item_type = str(row.get("type", "item") or "item").strip() or "item"
        item_id = str(row.get("id", "") or "").strip()
        status = str(row.get("status", "") or "").strip()
        detail = str(row.get("detail", "") or "").strip()
        lines.append(f"- {item_type} {item_id}".rstrip())
        if status:
            lines.append(f"  status: {status}")
        if detail:
            for part in [piece.strip() for piece in detail.split(";") if piece.strip()]:
                normalized = part
                if part.startswith("cmd="):
                    normalized = f"command: {part[4:]}"
                elif part.startswith("status="):
                    normalized = f"status: {part[7:]}"
                elif part.startswith("path="):
                    normalized = f"path: {part[5:]}"
                elif part.startswith("query="):
                    normalized = f"query: {part[6:]}"
                elif part.startswith("action="):
                    normalized = f"action: {part[7:]}"
                elif part.startswith("output_chars="):
                    normalized = f"output chars: {part[13:]}"
                lines.append(f"  {normalized}")


def _normalize_text(value: object) -> str:
    if not isinstance(value, str):
        return ""
    return value.strip()


def _normalize_status(value: object) -> tuple[str, list[str]]:
    if isinstance(value, str):
        normalized = value.strip()
        return (normalized, []) if normalized else ("", [])
    if not isinstance(value, dict):
        return ("", [])
    status_type = _normalize_text(value.get("type"))
    raw_flags = value.get("activeFlags")
    active_flags: list[str] = []
    if isinstance(raw_flags, list):
        for item in raw_flags:
            if isinstance(item, str) and item.strip():
                active_flags.append(item.strip())
    return status_type, active_flags


def _normalize_loaded(value: object) -> str:
    if isinstance(value, bool):
        return "yes" if value else "no"
    return ""


def _normalize_bool_label(value: object) -> str:
    if isinstance(value, bool):
        return "yes" if value else "no"
    return ""


def _format_timestamp(value: object) -> str:
    if isinstance(value, str) and value.strip():
        return value.strip()
    if isinstance(value, (int, float)):
        try:
            dt = datetime.fromtimestamp(float(value), tz=timezone.utc)
        except (OverflowError, OSError, ValueError):
            return ""
        return dt.isoformat().replace("+00:00", "Z")
    return ""
