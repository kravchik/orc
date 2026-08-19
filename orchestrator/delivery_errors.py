"""Shared extraction and logging fields for access-point delivery failures."""

from __future__ import annotations

import re


def extract_http_code(exc: Exception) -> int | None:
    raw_code = getattr(exc, "http_code", None)
    if isinstance(raw_code, int):
        return raw_code
    match = re.search(r"http error:\s*(\d+)", str(exc), flags=re.IGNORECASE)
    if match is None:
        return None
    try:
        return int(match.group(1))
    except ValueError:
        return None
