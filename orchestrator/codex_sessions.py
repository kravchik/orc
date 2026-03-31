"""Helpers for reading Codex CLI session metadata."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from dataclasses import dataclass
from pathlib import Path
from typing import Iterator


@dataclass(frozen=True)
class CodexCliSession:
    session_id: str
    cwd: str
    last_activity_epoch: float
    last_activity_iso: str
    source_path: str
    model_provider: str = ""
    originator: str = ""
    source: str = ""
    cli_version: str = ""
    thread_name: str = ""
    thread_name_updated_at: str = ""


def find_latest_codex_cli_session_id(
    *,
    project_cwd: str | None = None,
    sessions_root: str | Path | None = None,
) -> str | None:
    """Return the newest Codex CLI session id for current project.

    The function scans `~/.codex/sessions/**/*.jsonl`, reads `session_meta`,
    filters by exact resolved cwd match when `project_cwd` is provided, and
    returns the newest session id by last activity timestamp.
    """

    sessions = list_codex_cli_sessions(
        project_cwd=project_cwd,
        sessions_root=sessions_root,
        limit=1,
    )
    if not sessions:
        return None
    return sessions[0].session_id


def list_codex_cli_sessions(
    *,
    project_cwd: str | None = None,
    sessions_root: str | Path | None = None,
    limit: int = 20,
) -> list[CodexCliSession]:
    root = _resolve_sessions_root(sessions_root)
    if not root.exists() or not root.is_dir():
        return []

    thread_name_by_id = _load_latest_thread_names(root.parent / "session_index.jsonl")
    target_cwd = _resolve_path(project_cwd) if project_cwd else None
    items: list[CodexCliSession] = []
    for file_path in _iter_session_files(root):
        meta = _read_session_meta(file_path)
        if meta is None:
            continue
        session_id, cwd, created_ts, extra = meta
        thread_name, thread_name_updated_at = thread_name_by_id.get(session_id, ("", ""))
        resolved_cwd = _resolve_path(cwd)
        if target_cwd is not None and resolved_cwd != target_cwd:
            continue
        activity_ts = _resolve_last_activity_ts(file_path, created_ts)
        items.append(
            CodexCliSession(
                session_id=session_id,
                cwd=resolved_cwd,
                last_activity_epoch=activity_ts,
                last_activity_iso=_format_timestamp(activity_ts),
                source_path=str(file_path.resolve()),
                model_provider=extra.get("model_provider", ""),
                originator=extra.get("originator", ""),
                source=extra.get("source", ""),
                cli_version=extra.get("cli_version", ""),
                thread_name=thread_name,
                thread_name_updated_at=thread_name_updated_at,
            )
        )
    items.sort(key=lambda item: item.last_activity_epoch, reverse=True)
    safe_limit = max(0, int(limit))
    if safe_limit == 0:
        return []
    return items[:safe_limit]


def _resolve_sessions_root(value: str | Path | None) -> Path:
    if value is None:
        return (Path.home() / ".codex" / "sessions").resolve()
    return Path(value).expanduser().resolve()


def _load_latest_thread_names(index_path: Path) -> dict[str, tuple[str, str]]:
    if not index_path.exists() or not index_path.is_file():
        return {}

    best_by_id: dict[str, tuple[str, str, float, int]] = {}
    try:
        with index_path.open("r", encoding="utf-8") as fh:
            for line_no, line in enumerate(fh, 1):
                raw = line.strip()
                if not raw:
                    continue
                try:
                    record = json.loads(raw)
                except json.JSONDecodeError:
                    continue
                if not isinstance(record, dict):
                    continue
                session_id = record.get("id")
                thread_name = record.get("thread_name")
                updated_at = record.get("updated_at")
                if not isinstance(session_id, str) or not session_id.strip():
                    continue
                if not isinstance(thread_name, str) or not thread_name.strip():
                    continue
                updated_at_str = updated_at.strip() if isinstance(updated_at, str) else ""
                ts = _parse_ts(updated_at_str)
                key = session_id.strip()
                prev = best_by_id.get(key)
                if prev is None:
                    best_by_id[key] = (thread_name.strip(), updated_at_str, ts, line_no)
                    continue
                prev_name, prev_updated_at, prev_ts, prev_line_no = prev
                _ = (prev_name, prev_updated_at)
                if ts > prev_ts or (ts == prev_ts and line_no > prev_line_no):
                    best_by_id[key] = (thread_name.strip(), updated_at_str, ts, line_no)
    except OSError:
        return {}

    return {
        session_id: (thread_name, updated_at_str)
        for session_id, (thread_name, updated_at_str, _ts, _line_no) in best_by_id.items()
    }


def _iter_session_files(root: Path) -> Iterator[Path]:
    try:
        for path in root.rglob("*.jsonl"):
            if path.is_file():
                yield path
    except OSError:
        return


def _read_session_meta(path: Path) -> tuple[str, str, float, dict[str, str]] | None:
    try:
        with path.open("r", encoding="utf-8") as fh:
            for _ in range(8):
                line = fh.readline()
                if not line:
                    break
                line = line.strip()
                if not line:
                    continue
                try:
                    record = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if record.get("type") != "session_meta":
                    continue
                payload = record.get("payload")
                if not isinstance(payload, dict):
                    return None
                session_id = payload.get("id")
                cwd = payload.get("cwd")
                if not isinstance(session_id, str) or not session_id.strip():
                    return None
                if not isinstance(cwd, str) or not cwd.strip():
                    return None
                ts = _parse_ts(payload.get("timestamp"))
                if ts <= 0.0:
                    ts = _parse_ts(record.get("timestamp"))
                if ts <= 0.0:
                    ts = path.stat().st_mtime
                return (
                    session_id.strip(),
                    cwd.strip(),
                    ts,
                    {
                        "model_provider": _payload_str(payload, "model_provider"),
                        "originator": _payload_str(payload, "originator"),
                        "source": _payload_str(payload, "source"),
                        "cli_version": _payload_str(payload, "cli_version"),
                    },
                )
    except OSError:
        return None
    return None


def _parse_ts(raw: object) -> float:
    if not isinstance(raw, str):
        return 0.0
    text = raw.strip()
    if not text:
        return 0.0
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        return datetime.fromisoformat(text).timestamp()
    except ValueError:
        return 0.0


def _resolve_path(raw: str) -> str:
    try:
        return str(Path(raw).expanduser().resolve())
    except OSError:
        return raw


def _format_timestamp(ts: float) -> str:
    if ts <= 0:
        return ""
    return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def _resolve_last_activity_ts(path: Path, created_ts: float) -> float:
    try:
        file_mtime = path.stat().st_mtime
    except OSError:
        file_mtime = 0.0
    return max(file_mtime, created_ts)


def _payload_str(payload: dict, key: str) -> str:
    raw = payload.get(key)
    if isinstance(raw, str):
        return raw.strip()
    return ""
