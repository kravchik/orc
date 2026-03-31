from __future__ import annotations

import threading
import time


class _GlobalClock:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._offset_sec = 0.0
        self._paused = False
        self._paused_value = 0.0

    def monotonic(self) -> float:
        with self._lock:
            if self._paused:
                return float(self._paused_value)
            return float(time.monotonic() + self._offset_sec)

    def pause(self) -> float:
        with self._lock:
            if not self._paused:
                self._paused_value = float(time.monotonic() + self._offset_sec)
                self._paused = True
            return float(self._paused_value)

    def resume(self) -> float:
        with self._lock:
            if self._paused:
                self._offset_sec = float(self._paused_value - time.monotonic())
                self._paused = False
            return float(time.monotonic() + self._offset_sec)

    def advance(self, delta_sec: float) -> float:
        with self._lock:
            delta = float(delta_sec)
            if self._paused:
                self._paused_value += delta
                return float(self._paused_value)
            self._offset_sec += delta
            return float(time.monotonic() + self._offset_sec)

    def set(self, ts: float) -> float:
        with self._lock:
            self._paused_value = float(ts)
            self._paused = True
            return float(self._paused_value)

    def reset(self) -> None:
        with self._lock:
            self._offset_sec = 0.0
            self._paused = False
            self._paused_value = 0.0

    def is_paused(self) -> bool:
        with self._lock:
            return bool(self._paused)


_CLOCK = _GlobalClock()


def monotonic() -> float:
    return _CLOCK.monotonic()


def pause() -> float:
    return _CLOCK.pause()


def resume() -> float:
    return _CLOCK.resume()


def advance(delta_sec: float) -> float:
    return _CLOCK.advance(delta_sec)


def set(ts: float) -> float:
    return _CLOCK.set(ts)


def reset() -> None:
    _CLOCK.reset()


def is_paused() -> bool:
    return _CLOCK.is_paused()
