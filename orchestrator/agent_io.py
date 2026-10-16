"""Provider-neutral structured agent input and output values."""

from __future__ import annotations

from dataclasses import dataclass, replace
from enum import StrEnum
from pathlib import PurePosixPath


UPLOADS_RELATIVE_DIR = PurePosixPath(".orc/uploads")


class AgentInputOrigin(StrEnum):
    HUMAN = "human"
    CONTROL = "control"
    ROUTING = "routing"


def normalize_orc_upload_path(value: str) -> str:
    normalized = str(value or "").strip()
    raw_parts = normalized.split("/")
    path = PurePosixPath(normalized)
    if (
        not normalized
        or path.is_absolute()
        or len(raw_parts) != 3
        or tuple(raw_parts[:2]) != tuple(UPLOADS_RELATIVE_DIR.parts)
        or raw_parts[2] in {"", ".", ".."}
    ):
        raise ValueError("agent file reference must use .orc/uploads/<filename>")
    return path.as_posix()


@dataclass(frozen=True)
class AgentAttachmentRef:
    relative_path: str
    display_name: str
    media_type: str
    size_bytes: int

    def __post_init__(self) -> None:
        object.__setattr__(self, "relative_path", normalize_orc_upload_path(self.relative_path))
        if not str(self.display_name or "").strip():
            raise ValueError("attachment display name must not be empty")
        if not str(self.media_type or "").strip():
            raise ValueError("attachment media type must not be empty")
        if self.size_bytes < 0:
            raise ValueError("attachment size must not be negative")


@dataclass(frozen=True)
class AgentInput:
    text: str
    origin: AgentInputOrigin
    verbatim: bool = False
    attachments: tuple[AgentAttachmentRef, ...] = ()

    def with_text(self, text: str) -> AgentInput:
        return replace(self, text=str(text))


@dataclass(frozen=True)
class AgentOutput:
    text: str
