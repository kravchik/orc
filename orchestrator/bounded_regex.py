"""Interruptible regular-expression matching for untrusted allowlist rules."""

from __future__ import annotations

from dataclasses import dataclass
import json
import re
import signal
import subprocess
import sys
import threading
import time


@dataclass(frozen=True)
class BoundedRegexSearchResult:
    matched: bool
    timed_out: bool
    duration_sec: float
    error: str = ""


class _RegexMatchTimedOut(Exception):
    pass


def _raise_regex_match_timeout(_signum, _frame) -> None:
    raise _RegexMatchTimedOut()


def search_regex_with_timeout(
    *,
    pattern: re.Pattern[str],
    value: str,
    timeout_sec: float,
) -> BoundedRegexSearchResult:
    if (
        threading.current_thread() is threading.main_thread()
        and hasattr(signal, "SIGALRM")
        and hasattr(signal, "getitimer")
        and hasattr(signal, "setitimer")
        and signal.getitimer(signal.ITIMER_REAL)[0] <= 0
    ):
        return _search_regex_with_signal(
            pattern=pattern,
            value=value,
            timeout_sec=timeout_sec,
        )
    return _search_regex_in_subprocess(
        pattern=pattern,
        value=value,
        timeout_sec=timeout_sec,
    )


def _search_regex_with_signal(
    *,
    pattern: re.Pattern[str],
    value: str,
    timeout_sec: float,
) -> BoundedRegexSearchResult:
    started_at = time.monotonic()
    previous_handler = signal.getsignal(signal.SIGALRM)
    signal.signal(signal.SIGALRM, _raise_regex_match_timeout)
    signal.setitimer(signal.ITIMER_REAL, timeout_sec)
    try:
        matched = pattern.search(value) is not None
        return BoundedRegexSearchResult(
            matched=matched,
            timed_out=False,
            duration_sec=time.monotonic() - started_at,
        )
    except _RegexMatchTimedOut:
        return BoundedRegexSearchResult(
            matched=False,
            timed_out=True,
            duration_sec=time.monotonic() - started_at,
        )
    except Exception as exc:
        return BoundedRegexSearchResult(
            matched=False,
            timed_out=False,
            duration_sec=time.monotonic() - started_at,
            error=f"{type(exc).__name__}: {exc}",
        )
    finally:
        signal.setitimer(signal.ITIMER_REAL, 0)
        signal.signal(signal.SIGALRM, previous_handler)


def _search_regex_in_subprocess(
    *,
    pattern: re.Pattern[str],
    value: str,
    timeout_sec: float,
) -> BoundedRegexSearchResult:
    payload = json.dumps(
        {"pattern": pattern.pattern, "flags": pattern.flags, "value": value}
    )
    script = (
        "import json,re,sys; "
        "p=json.loads(sys.stdin.read()); "
        "sys.exit(0 if re.compile(p['pattern'],p['flags']).search(p['value']) else 1)"
    )
    started_at = time.monotonic()
    try:
        completed = subprocess.run(
            [sys.executable, "-c", script],
            input=payload,
            text=True,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
            timeout=timeout_sec,
            check=False,
        )
    except subprocess.TimeoutExpired:
        return BoundedRegexSearchResult(
            matched=False,
            timed_out=True,
            duration_sec=time.monotonic() - started_at,
        )
    duration_sec = time.monotonic() - started_at
    if completed.returncode in (0, 1):
        return BoundedRegexSearchResult(
            matched=completed.returncode == 0,
            timed_out=False,
            duration_sec=duration_sec,
        )
    error = (completed.stderr or "regex subprocess failed").strip()
    return BoundedRegexSearchResult(
        matched=False,
        timed_out=False,
        duration_sec=duration_sec,
        error=error,
    )
