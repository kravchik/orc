"""Safe on-disk storage and prompt formatting for chat uploads."""

from __future__ import annotations

from dataclasses import dataclass
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import re
import tempfile


UPLOADS_RELATIVE_DIR = PurePosixPath(".orc/uploads")
MAX_UPLOAD_FILE_BYTES = 50 * 1024 * 1024
MAX_PENDING_UPLOAD_BYTES = 100 * 1024 * 1024
MAX_PENDING_UPLOADS = 100


@dataclass(frozen=True)
class IncomingUpload:
    source_id: str
    preferred_name: str
    declared_size: int | None
    download_ref: str


@dataclass(frozen=True)
class PendingUpload:
    relative_path: str
    size_bytes: int

    @property
    def name(self) -> str:
        return PurePosixPath(self.relative_path).name

    def to_payload(self) -> dict[str, object]:
        return {
            "relative_path": self.relative_path,
            "size_bytes": self.size_bytes,
        }


@dataclass(frozen=True)
class SavedUpload:
    pending: PendingUpload
    absolute_path: Path


class PendingUploadStore:
    """Persist one proxy access point's pending upload batch in its project."""

    def __init__(self, *, project_cwd: str | Path, namespace: str) -> None:
        self._project_cwd = Path(project_cwd).expanduser().resolve(strict=True)
        if not self._project_cwd.is_dir():
            raise ValueError("working directory is not a directory")
        digest = hashlib.sha256(namespace.encode("utf-8")).hexdigest()[:24]
        self._manifest_name = f".pending-v1-{digest}.json"
        self._pending = self._load()

    @property
    def pending(self) -> tuple[PendingUpload, ...]:
        return self._pending

    def validate(self, *, declared_size: int | None) -> None:
        validate_pending_capacity(self._pending, declared_size=declared_size)

    def save(self, *, preferred_name: str, content: bytes) -> tuple[int, SavedUpload]:
        validate_pending_capacity(self._pending, declared_size=len(content))
        saved = save_upload(
            project_cwd=self._project_cwd,
            preferred_name=preferred_name,
            content=content,
        )
        updated = (*self._pending, saved.pending)
        try:
            self._persist(updated)
        except BaseException:
            saved.absolute_path.unlink(missing_ok=True)
            raise
        self._pending = updated
        return (len(updated), saved)

    def consume(self, *, text: str) -> str:
        if not self._pending:
            return text
        reserved = self._pending
        self._persist(())
        self._pending = ()
        return format_upload_prompt(uploads=reserved, text=text)

    def _manifest_path(self, *, create_directory: bool) -> Path:
        uploads_dir = _resolve_uploads_dir(
            project_cwd=self._project_cwd,
            create=create_directory,
        )
        return uploads_dir / self._manifest_name

    def _load(self) -> tuple[PendingUpload, ...]:
        path = self._manifest_path(create_directory=False)
        if not path.exists():
            return ()
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise ValueError(f"invalid pending upload manifest: {path}") from exc
        if not isinstance(payload, dict) or payload.get("version") != 1:
            raise ValueError(f"invalid pending upload manifest: {path}")
        raw_pending = payload.get("pending")
        if not isinstance(raw_pending, list):
            raise ValueError(f"invalid pending upload manifest: {path}")
        pending: list[PendingUpload] = []
        for raw in raw_pending:
            item = pending_upload_from_payload(raw)
            if item is None:
                raise ValueError(f"invalid pending upload manifest: {path}")
            pending.append(item)
        validate_loaded_pending(pending)
        return tuple(pending)

    def _persist(self, pending: tuple[PendingUpload, ...]) -> None:
        path = self._manifest_path(create_directory=True)
        payload = {
            "version": 1,
            "pending": [item.to_payload() for item in pending],
        }
        fd, temporary_name = tempfile.mkstemp(
            prefix=f"{self._manifest_name}.",
            suffix=".tmp",
            dir=path.parent,
        )
        temporary_path = Path(temporary_name)
        try:
            os.fchmod(fd, 0o600)
            with os.fdopen(fd, "w", encoding="utf-8") as stream:
                json.dump(payload, stream, ensure_ascii=True, separators=(",", ":"))
                stream.write("\n")
                stream.flush()
                os.fsync(stream.fileno())
            os.replace(temporary_path, path)
        except BaseException:
            try:
                os.close(fd)
            except OSError:
                pass
            temporary_path.unlink(missing_ok=True)
            raise


def slack_uploads_from_message(message: dict[str, object]) -> tuple[IncomingUpload, ...]:
    files = message.get("files")
    if not isinstance(files, list):
        return ()
    uploads: list[IncomingUpload] = []
    for value in files:
        if not isinstance(value, dict):
            continue
        source_id = str(value.get("id") or "")
        preferred_name = str(
            value.get("name") or value.get("title") or source_id or "file"
        )
        raw_size = value.get("size")
        try:
            declared_size = (
                int(raw_size)
                if isinstance(raw_size, (int, str)) and str(raw_size).strip()
                else None
            )
        except ValueError:
            declared_size = None
        download_ref = str(
            value.get("url_private_download") or value.get("url_private") or ""
        ).strip()
        uploads.append(
            IncomingUpload(
                source_id=source_id,
                preferred_name=preferred_name,
                declared_size=declared_size,
                download_ref=download_ref,
            )
        )
    return tuple(uploads)


def pending_upload_from_payload(value: object) -> PendingUpload | None:
    if not isinstance(value, dict):
        return None
    relative_path = str(value.get("relative_path") or "").strip()
    try:
        size_bytes = int(value.get("size_bytes") or 0)
    except (TypeError, ValueError):
        return None
    path = PurePosixPath(relative_path)
    if (
        size_bytes < 0
        or path.is_absolute()
        or ".." in path.parts
        or tuple(path.parts[:2]) != tuple(UPLOADS_RELATIVE_DIR.parts)
        or len(path.parts) != 3
    ):
        return None
    return PendingUpload(relative_path=path.as_posix(), size_bytes=size_bytes)


def validate_pending_capacity(
    pending: tuple[PendingUpload, ...] | list[PendingUpload],
    *,
    declared_size: int | None,
) -> None:
    if len(pending) >= MAX_PENDING_UPLOADS:
        raise ValueError(f"pending upload limit reached ({MAX_PENDING_UPLOADS} files)")
    if declared_size is not None:
        if declared_size < 0:
            raise ValueError("upload size must not be negative")
        if declared_size > MAX_UPLOAD_FILE_BYTES:
            raise ValueError(f"file exceeds upload limit ({MAX_UPLOAD_FILE_BYTES} bytes)")
        current_size = sum(item.size_bytes for item in pending)
        if current_size + declared_size > MAX_PENDING_UPLOAD_BYTES:
            raise ValueError(
                f"pending upload batch exceeds limit ({MAX_PENDING_UPLOAD_BYTES} bytes)"
            )


def validate_loaded_pending(pending: list[PendingUpload]) -> None:
    if len(pending) > MAX_PENDING_UPLOADS:
        raise ValueError(f"pending upload limit exceeded ({MAX_PENDING_UPLOADS} files)")
    if any(item.size_bytes > MAX_UPLOAD_FILE_BYTES for item in pending):
        raise ValueError(f"file exceeds upload limit ({MAX_UPLOAD_FILE_BYTES} bytes)")
    if sum(item.size_bytes for item in pending) > MAX_PENDING_UPLOAD_BYTES:
        raise ValueError(f"pending upload batch exceeds limit ({MAX_PENDING_UPLOAD_BYTES} bytes)")


def save_upload(
    *,
    project_cwd: str | Path,
    preferred_name: str,
    content: bytes,
) -> SavedUpload:
    if len(content) > MAX_UPLOAD_FILE_BYTES:
        raise ValueError(f"file exceeds upload limit ({MAX_UPLOAD_FILE_BYTES} bytes)")
    cwd = Path(project_cwd).expanduser().resolve(strict=True)
    resolved_uploads_dir = _resolve_uploads_dir(project_cwd=cwd, create=True)

    safe_name = sanitize_upload_name(preferred_name)
    destination = _unique_destination(resolved_uploads_dir, safe_name)
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
    fd = os.open(destination, flags, 0o600)
    try:
        with os.fdopen(fd, "wb") as stream:
            stream.write(content)
            stream.flush()
            os.fsync(stream.fileno())
    except BaseException:
        destination.unlink(missing_ok=True)
        raise
    relative = destination.relative_to(cwd).as_posix()
    return SavedUpload(
        pending=PendingUpload(relative_path=relative, size_bytes=len(content)),
        absolute_path=destination,
    )


def sanitize_upload_name(value: str) -> str:
    raw = str(value or "").replace("\\", "/")
    name = PurePosixPath(raw).name.strip()
    name = "".join("_" if ord(char) < 32 or ord(char) == 127 else char for char in name)
    name = re.sub(r"\s+", " ", name).strip(" .")
    if name in {"", ".", ".."}:
        name = "upload"
    if len(name) > 180:
        suffix = Path(name).suffix[:30]
        stem_limit = max(1, 180 - len(suffix))
        name = f"{Path(name).stem[:stem_limit]}{suffix}"
    return name


def format_upload_prompt(*, uploads: tuple[PendingUpload, ...], text: str) -> str:
    if not uploads:
        return text
    lines = [
        "К сообщению приложены файлы, сохранённые в рабочей директории:",
        *(f"{index}. {item.relative_path}" for index, item in enumerate(uploads, start=1)),
        "",
        "Сообщение пользователя:",
        text,
    ]
    return "\n".join(lines)


def _unique_destination(directory: Path, name: str) -> Path:
    candidate = directory / name
    if not candidate.exists():
        return candidate
    path = Path(name)
    suffix = path.suffix
    stem = path.stem or "upload"
    sequence = 2
    while True:
        candidate = directory / f"{stem}-{sequence}{suffix}"
        if not candidate.exists():
            return candidate
        sequence += 1


def _resolve_uploads_dir(*, project_cwd: str | Path, create: bool) -> Path:
    cwd = Path(project_cwd).expanduser().resolve(strict=True)
    if not cwd.is_dir():
        raise ValueError("bound working directory is not a directory")
    orc_dir = cwd / UPLOADS_RELATIVE_DIR.parts[0]
    if create:
        orc_dir.mkdir(exist_ok=True)
    if not orc_dir.exists():
        return cwd.joinpath(*UPLOADS_RELATIVE_DIR.parts)
    resolved_orc_dir = orc_dir.resolve(strict=True)
    if not resolved_orc_dir.is_relative_to(cwd):
        raise ValueError("upload directory resolves outside the bound working directory")
    uploads_dir = resolved_orc_dir / UPLOADS_RELATIVE_DIR.parts[1]
    if create:
        uploads_dir.mkdir(exist_ok=True)
    if not uploads_dir.exists():
        return uploads_dir
    resolved_uploads_dir = uploads_dir.resolve(strict=True)
    if not resolved_uploads_dir.is_relative_to(cwd):
        raise ValueError("upload directory resolves outside the bound working directory")
    return resolved_uploads_dir
