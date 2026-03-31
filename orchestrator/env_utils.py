"""Shared helpers for reading local KEY=VALUE files and resolving env values."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Mapping


def load_env_file(path: str) -> dict[str, str]:
    env: dict[str, str] = {}
    p = Path(path)
    if not p.exists():
        return env
    for raw in p.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        env[key.strip()] = value.strip()
    return env


def resolve_env(raw: str, env_key: str, file_env: Mapping[str, str], default: str = "") -> str:
    value = raw.strip()
    if value:
        return value
    value = os.getenv(env_key, "").strip()
    if value:
        return value
    value = str(file_env.get(env_key, "")).strip()
    if value:
        return value
    return default
