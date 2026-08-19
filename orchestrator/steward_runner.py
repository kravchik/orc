"""Shared runner/bootstrap helpers for steward-style runtimes."""

from __future__ import annotations

import time
from typing import Callable, Protocol

from orchestrator import clock
from orchestrator.processes import LifecycleLogger
from orchestrator.steward_runtime_support import (
    StopStewardLoop,
    StewardTickLoop,
)


Writer = Callable[[str], None]
STEWARD_IDLE_SLEEP_SEC = 0.1
STEWARD_SLOW_TICK_WARNING_SEC = 0.1


class StewardTransportAdapter(Protocol):
    stopping_message: str
    sigint_event_name: str
    stopped_event_name: str

    def should_stop_after_tick(self) -> bool: ...
    def drain_on_forced_stop(self, loop: StewardTickLoop) -> int: ...
    def handle_stop(self, loop: StewardTickLoop, stop: StopStewardLoop) -> int: ...
    def handle_error(self, exc: Exception) -> int: ...
    def stop_resources(self) -> None: ...




def run_steward_runtime(
    *,
    loop: StewardTickLoop,
    transport: StewardTransportAdapter,
    writer: Writer,
    logger: LifecycleLogger,
    sleep_fn: Callable[[float], None] = time.sleep,
    monotonic_now: Callable[[], float] = clock.monotonic,
) -> int:
    try:
        while True:
            tick_started_at = float(monotonic_now())
            progressed = loop.tick_once()
            tick_duration_sec = float(monotonic_now()) - tick_started_at
            if tick_duration_sec >= STEWARD_SLOW_TICK_WARNING_SEC:
                logger.event(
                    "steward_slow_tick",
                    duration_ms=round(tick_duration_sec * 1000.0, 3),
                    progressed=progressed,
                )
            if transport.should_stop_after_tick():
                return transport.drain_on_forced_stop(loop)
            if not progressed:
                sleep_fn(STEWARD_IDLE_SLEEP_SEC)
    except StopStewardLoop as stop:
        return transport.handle_stop(loop, stop)
    except KeyboardInterrupt:
        writer("^C")
        writer(transport.stopping_message)
        logger.event(transport.sigint_event_name)
        return 130
    except Exception as exc:
        return transport.handle_error(exc)
    finally:
        transport.stop_resources()
        logger.event(transport.stopped_event_name)
