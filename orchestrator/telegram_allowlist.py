"""Reloadable Telegram chat allowlist backed by a local env file."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from orchestrator.env_utils import load_env_file


def parse_telegram_allowed_chat_ids(raw: str, *, setting_name: str) -> set[int]:
    value = raw.strip()
    if not value:
        raise ValueError(f"{setting_name} is required and must be non-empty")
    try:
        parsed = {int(part.strip()) for part in value.split(",") if part.strip()}
    except ValueError as exc:
        raise ValueError(f"{setting_name} must be comma-separated integers") from exc
    if not parsed:
        raise ValueError(f"{setting_name} is required and must be non-empty")
    return parsed


@dataclass(frozen=True)
class TelegramAllowlistReloadResult:
    reloaded: bool
    added_chat_ids: tuple[int, ...] = ()
    removed_chat_ids: tuple[int, ...] = ()
    error: str | None = None
    error_type: str | None = None


class ReloadableTelegramChatAllowlist:
    """Keeps the last valid allowlist and reparses only changed files."""

    def __init__(
        self,
        allowed_chat_ids: set[int],
    ) -> None:
        if not allowed_chat_ids:
            raise ValueError("allowed_chat_ids must be non-empty")
        self._allowed_chat_ids = frozenset(int(chat_id) for chat_id in allowed_chat_ids)
        self._reload_file: Path | None = None
        self._observed_fingerprint: tuple[object, ...] = ("disabled",)

    @classmethod
    def from_env_file(cls, path: str | Path) -> ReloadableTelegramChatAllowlist:
        reload_file = Path(path).expanduser().absolute()
        observed_fingerprint = cls._file_fingerprint(reload_file)
        if observed_fingerprint[0] == "error":
            raise OSError(str(observed_fingerprint[2]))
        file_env = load_env_file(str(reload_file))
        allowed_chat_ids = parse_telegram_allowed_chat_ids(
            str(file_env.get("TELEGRAM_ALLOWED_CHAT_IDS", "")),
            setting_name="TELEGRAM_ALLOWED_CHAT_IDS",
        )
        instance = cls(allowed_chat_ids)
        instance._reload_file = reload_file
        instance._observed_fingerprint = observed_fingerprint
        return instance

    @property
    def allowed_chat_ids(self) -> set[int]:
        return set(self._allowed_chat_ids)

    @property
    def reload_file(self) -> Path | None:
        return self._reload_file

    def refresh_if_changed(self) -> TelegramAllowlistReloadResult | None:
        if self._reload_file is None:
            return None
        fingerprint = self._fingerprint()
        if fingerprint == self._observed_fingerprint:
            return None
        self._observed_fingerprint = fingerprint
        if fingerprint[0] == "error":
            error_type = str(fingerprint[1])
            error = str(fingerprint[2])
            return TelegramAllowlistReloadResult(
                reloaded=False,
                error=error,
                error_type=error_type,
            )
        try:
            file_env = load_env_file(str(self._reload_file))
            updated = frozenset(
                parse_telegram_allowed_chat_ids(
                    str(file_env.get("TELEGRAM_ALLOWED_CHAT_IDS", "")),
                    setting_name="TELEGRAM_ALLOWED_CHAT_IDS",
                )
            )
        except (OSError, UnicodeError, ValueError) as exc:
            return TelegramAllowlistReloadResult(
                reloaded=False,
                error=str(exc),
                error_type=type(exc).__name__,
            )
        previous = self._allowed_chat_ids
        self._allowed_chat_ids = updated
        return TelegramAllowlistReloadResult(
            reloaded=True,
            added_chat_ids=tuple(sorted(updated.difference(previous))),
            removed_chat_ids=tuple(sorted(previous.difference(updated))),
        )

    def _fingerprint(self) -> tuple[object, ...]:
        if self._reload_file is None:
            return ("disabled",)
        return self._file_fingerprint(self._reload_file)

    @staticmethod
    def _file_fingerprint(path: Path) -> tuple[object, ...]:
        try:
            stat = path.stat()
        except OSError as exc:
            return ("error", type(exc).__name__, str(exc))
        return ("file", stat.st_mtime_ns, stat.st_ctime_ns, stat.st_size, stat.st_ino)
