"""Transport-agnostic status output contract shared by AP-specific runtimes."""

from __future__ import annotations

from typing import Protocol


MAX_IN_PROGRESS_STATUS_WINDOWS = 10


def status_ref_log_fields(status_key: str) -> dict[str, object]:
    parts = str(status_key or "").split(":")
    if len(parts) >= 4 and parts[0] == "turn" and parts[1] in {"orc", "work"}:
        try:
            sequence = int(parts[3])
        except ValueError:
            return {}
        return {
            "status_ref_kind": "turn" if parts[1] == "orc" else "work",
            "status_ref_sequence": sequence,
        }
    return {}


class StatusOutputRuntime(Protocol):
    def append_status(
        self,
        status_text: str,
        *,
        status_key: str | None = None,
        force_new_if_not_latest: bool = False,
    ) -> None: ...

    def set_status_snapshot(
        self,
        status_text: str,
        *,
        status_key: str | None = None,
        force_new_if_not_latest: bool = False,
    ) -> None: ...

    def clear_status(self) -> None: ...

    def flush_status(self, *, status_key: str | None = None) -> None: ...

    def flush_due_statuses(self) -> bool: ...

    def set_status_in_progress(self, *, status_key: str, in_progress: bool) -> None: ...


class StructuredStatusOutputRuntime(Protocol):
    def set_status_snapshot_structured(
        self,
        *,
        status_key: str | None,
        meta_lines: list[str],
        snapshot: list[dict[str, str]],
    ) -> None: ...
