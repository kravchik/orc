"""Helpers for reliable small-file writes."""

from __future__ import annotations

import os
from pathlib import Path
from uuid import uuid4


def write_text_atomic(path: Path | str, text: str, *, encoding: str = "utf-8") -> None:
    """Atomically write text to a file in the same directory.

    The implementation writes to a temp file, fsyncs it, then replaces
    the destination path with os.replace.
    """

    dest = Path(path)
    dest.parent.mkdir(parents=True, exist_ok=True)
    tmp = dest.parent / f".{dest.name}.tmp.{os.getpid()}.{uuid4().hex}"
    try:
        with tmp.open("w", encoding=encoding) as handle:
            handle.write(text)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(tmp, dest)
        _fsync_dir(dest.parent)
    finally:
        try:
            if tmp.exists():
                tmp.unlink()
        except OSError:
            pass


def _fsync_dir(path: Path) -> None:
    if not hasattr(os, "O_DIRECTORY"):
        return
    fd: int | None = None
    try:
        fd = os.open(str(path), os.O_RDONLY | os.O_DIRECTORY)
        os.fsync(fd)
    except OSError:
        return
    finally:
        if fd is not None:
            try:
                os.close(fd)
            except OSError:
                pass

