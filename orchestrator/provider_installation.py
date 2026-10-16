"""Cheap, process-free installation preflight for runtime profiles."""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
import importlib.util
import os
from pathlib import Path
import platform
import shlex
import shutil
from typing import Mapping

from orchestrator.agent_identity import ProviderExecutionProfile


_SHEBANG_MAX_BYTES = 512


@dataclass(frozen=True)
class RuntimeBundledExecutable:
    package: str
    posix_relative_path: str
    windows_relative_path: str

    def __post_init__(self) -> None:
        if not isinstance(self.package, str) or not self.package.strip():
            raise ValueError("bundled executable package must be non-empty")
        object.__setattr__(self, "package", self.package.strip())
        for field_name in ("posix_relative_path", "windows_relative_path"):
            value = getattr(self, field_name)
            if not isinstance(value, str) or not value.strip():
                raise ValueError("bundled executable path must be non-empty")
            relative = Path(value.strip())
            if relative.is_absolute() or ".." in relative.parts:
                raise ValueError("bundled executable path must stay package-relative")
            object.__setattr__(self, field_name, relative.as_posix())

    @property
    def relative_path(self) -> str:
        return (
            self.windows_relative_path
            if platform.system() == "Windows"
            else self.posix_relative_path
        )


@dataclass(frozen=True)
class RuntimeInstallationContract:
    command: tuple[str, ...]
    bundled_executable: RuntimeBundledExecutable | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.command, tuple):
            raise ValueError("runtime installation command must be a tuple")
        command = tuple(self.command)
        if not command or any(
            not isinstance(part, str) or not part.strip() or "\x00" in part
            for part in command
        ):
            raise ValueError("runtime installation command must be non-empty")
        if self.bundled_executable is not None and not isinstance(
            self.bundled_executable,
            RuntimeBundledExecutable,
        ):
            raise ValueError("runtime bundled executable declaration is invalid")
        object.__setattr__(self, "command", tuple(part.strip() for part in command))


class ProviderInstallationStatusKind(StrEnum):
    AVAILABLE = "available"
    MISSING_COMMAND = "missing_command"
    MISSING_DEPENDENCY = "missing_dependency"


@dataclass(frozen=True)
class ProviderInstallationStatus:
    execution_profile: ProviderExecutionProfile
    kind: ProviderInstallationStatusKind

    def __post_init__(self) -> None:
        if not isinstance(self.execution_profile, ProviderExecutionProfile):
            raise ValueError("installation status requires execution profile")
        if not isinstance(self.kind, ProviderInstallationStatusKind):
            raise ValueError("installation status kind must be typed")

    @property
    def available(self) -> bool:
        return self.kind is ProviderInstallationStatusKind.AVAILABLE


def preflight_runtime_installation(
    *,
    execution_profile: ProviderExecutionProfile,
    contract: RuntimeInstallationContract,
    configured_command: tuple[str, ...] | None,
    environment: Mapping[str, str],
) -> ProviderInstallationStatus:
    """Resolve one configured command and its local interpreter/path dependencies."""

    if not isinstance(contract, RuntimeInstallationContract):
        raise ValueError("installation preflight requires typed contract")
    if configured_command is not None and not isinstance(configured_command, tuple):
        raise ValueError("configured installation command must be a tuple")
    command = configured_command or contract.command
    if not command or any(not isinstance(part, str) or not part for part in command):
        return ProviderInstallationStatus(
            execution_profile,
            ProviderInstallationStatusKind.MISSING_COMMAND,
        )
    executable = None
    if (
        contract.bundled_executable is not None
        and configured_command in (None, contract.command)
    ):
        executable = _resolve_bundled_executable(contract.bundled_executable)
    if executable is None:
        executable = _resolve_executable(command[0], environment)
    if executable is None:
        return ProviderInstallationStatus(
            execution_profile,
            ProviderInstallationStatusKind.MISSING_COMMAND,
        )
    if not _local_dependencies_available(executable, command[1:], environment):
        return ProviderInstallationStatus(
            execution_profile,
            ProviderInstallationStatusKind.MISSING_DEPENDENCY,
        )
    return ProviderInstallationStatus(
        execution_profile,
        ProviderInstallationStatusKind.AVAILABLE,
    )


def _resolve_bundled_executable(
    declaration: RuntimeBundledExecutable,
) -> Path | None:
    spec = importlib.util.find_spec(declaration.package)
    if spec is None:
        return None
    roots = tuple(spec.submodule_search_locations or ())
    if not roots and spec.origin:
        roots = (str(Path(spec.origin).parent),)
    for root in roots:
        candidate = Path(root) / declaration.relative_path
        if _is_executable_file(candidate):
            return candidate
    return None


def _resolve_executable(raw: str, environment: Mapping[str, str]) -> Path | None:
    if Path(raw).is_absolute() or os.sep in raw or (os.altsep and os.altsep in raw):
        candidate = _environment_path(raw, environment)
        return candidate if _is_executable_file(candidate) else None
    resolved = shutil.which(raw, path=environment.get("PATH", ""))
    if not resolved:
        return None
    candidate = Path(resolved)
    return candidate if _is_executable_file(candidate) else None


def _is_executable_file(path: Path) -> bool:
    try:
        return path.is_file() and os.access(path, os.X_OK)
    except OSError:
        return False


def _local_dependencies_available(
    executable: Path,
    arguments: tuple[str, ...],
    environment: Mapping[str, str],
) -> bool:
    if not _shebang_dependency_available(executable, environment):
        return False
    for argument in arguments:
        if not _looks_like_local_path(argument):
            continue
        dependency = _environment_path(argument, environment)
        try:
            if not dependency.is_file() or not os.access(dependency, os.R_OK):
                return False
        except OSError:
            return False
    return True


def _environment_path(raw: str, environment: Mapping[str, str]) -> Path:
    tilde_prefixes = (f"~{os.sep}",) if not os.altsep else (f"~{os.sep}", f"~{os.altsep}")
    if raw == "~" or raw.startswith(tilde_prefixes):
        home = environment.get("HOME", "") or environment.get("USERPROFILE", "")
        if home:
            if raw == "~":
                return Path(home)
            return Path(home) / raw[2:]
    return Path(raw)


def _looks_like_local_path(value: str) -> bool:
    return bool(value) and not value.startswith("-") and (
        Path(value).is_absolute()
        or os.sep in value
        or bool(os.altsep and os.altsep in value)
    )


def _shebang_dependency_available(
    executable: Path,
    environment: Mapping[str, str],
) -> bool:
    try:
        with executable.open("rb") as stream:
            first = stream.read(_SHEBANG_MAX_BYTES).splitlines()[0]
    except (OSError, IndexError):
        return True
    if not first.startswith(b"#!"):
        return True
    try:
        parts = shlex.split(first[2:].decode("utf-8", errors="strict").strip())
    except (UnicodeDecodeError, ValueError):
        return False
    if not parts:
        return False
    interpreter = _resolve_executable(parts[0], environment)
    if interpreter is None:
        return False
    if interpreter.name != "env":
        return True
    env_parts = [part for part in parts[1:] if part != "-S"]
    while env_parts and env_parts[0].startswith("-"):
        env_parts.pop(0)
    if not env_parts:
        return False
    return _resolve_executable(env_parts[0], environment) is not None
