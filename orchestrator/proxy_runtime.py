"""Shared runtime helpers for proxy-style access-point runtimes."""

from __future__ import annotations

import time
from typing import Any, Callable, Generic, Hashable, Protocol, TypeVar

from orchestrator import clock
from orchestrator.proxy_access_point import dispatch_proxy_action


AccessPointKeyT = TypeVar("AccessPointKeyT", bound=Hashable)
CoreT = TypeVar("CoreT")
AccessPointT = TypeVar("AccessPointT")
DriverT = TypeVar("DriverT")
StatusEventT = TypeVar("StatusEventT")


class StopProxyRuntime(Exception):
    """Transport-specific request to terminate the polling loop with an exit code."""

    def __init__(self, rc: int) -> None:
        super().__init__(rc)
        self.rc = int(rc)


class ProxyTransportAdapter(Protocol):
    """Transport-specific polling setup around the shared proxy runtime."""

    def start(self) -> None: ...

    def stop(self) -> None: ...

    def consume_once(self) -> bool: ...

    def ready(self) -> bool: ...


class ProxyRuntimeBase(Generic[AccessPointKeyT, CoreT, AccessPointT, DriverT, StatusEventT]):
    """Shared backend-drain and idle-drain logic for proxy runtimes."""

    def __init__(
        self,
        *,
        core: CoreT,
        access_point: AccessPointT,
        backend_driver: DriverT,
    ) -> None:
        self._core = core
        self._access_point = access_point
        self._backend_driver = backend_driver

    def _dispatch_proxy_action(self, action: object) -> None:
        dispatch_proxy_action(
            action=action,
            driver=self._backend_driver,
            approval_state=self._core,
        )

    def _drain_backend_events(
        self,
        *,
        status_event_type: type[StatusEventT],
        handle_event: Callable[[object], None],
    ) -> bool:
        progressed = False
        pending_status: StatusEventT | None = None

        def _flush_pending_status() -> None:
            nonlocal pending_status
            if pending_status is None:
                return
            self._core.apply_status_update(pending_status)
            pending_status = None

        while True:
            events = self._backend_driver.poll_once()
            if not events:
                break
            progressed = True
            for event in events:
                if isinstance(event, status_event_type):
                    pending_status = event
                    continue
                _flush_pending_status()
                handle_event(event)
        _flush_pending_status()
        return progressed

    def _drain_until_idle(
        self,
        *,
        request_timeout_sec: float,
        consume_transport_once: Callable[[], bool],
    ) -> int:
        wait_deadline = clock.monotonic() + max(0.5, float(request_timeout_sec) + 0.5)
        while not self._core.is_awaiting() and clock.monotonic() < wait_deadline:
            progressed = self._drain_driver_once()
            progressed = self._core.flush_delivery_lane() or progressed
            progressed = self._core.flush_due_statuses() or progressed
            progressed = consume_transport_once() or progressed
            progressed = self._core.flush_delivery_lane() or progressed
            progressed = self._core.flush_due_statuses() or progressed
            if not progressed:
                time.sleep(0.02)
        self._drain_driver_once()
        self._core.flush_delivery_lane()
        self._core.flush_due_statuses()
        return 0

    def _drain_driver_once(self) -> bool:
        raise NotImplementedError

    def _run_polling_loop(
        self,
        *,
        max_poll_cycles: int | None,
        request_timeout_sec: float,
        sleep_sec: float,
        sleep_after_progress: bool,
        before_run: Callable[[], None] | None = None,
        after_run: Callable[[], None] | None = None,
    ) -> int:
        self._core.set_awaiting(True)
        idle_cycles = 0
        if before_run is not None:
            before_run()
        try:
            while True:
                progressed = False
                progressed = self._drain_driver_once() or progressed
                progressed = self._core.flush_delivery_lane() or progressed
                progressed = self._core.flush_due_statuses() or progressed
                if self._transport_ready():
                    progressed = self._consume_transport_once() or progressed
                    progressed = self._core.flush_delivery_lane() or progressed
                    progressed = self._core.flush_due_statuses() or progressed
                if progressed:
                    idle_cycles = 0
                else:
                    idle_cycles += 1
                if max_poll_cycles is not None and idle_cycles >= max_poll_cycles:
                    return self._drain_until_idle(
                        request_timeout_sec=request_timeout_sec,
                        consume_transport_once=self._consume_transport_once,
                    )
                if (not progressed) or sleep_after_progress:
                    time.sleep(max(0.0, float(sleep_sec)))
        finally:
            if after_run is not None:
                after_run()

    def _consume_transport_once(self) -> bool:
        raise NotImplementedError

    def _transport_ready(self) -> bool:
        return bool(self._backend_driver.is_ready())


class ProxyAgentRuntime(
    ProxyRuntimeBase[AccessPointKeyT, CoreT, AccessPointT, DriverT, StatusEventT],
    Generic[AccessPointKeyT, CoreT, AccessPointT, DriverT, StatusEventT],
):
    """Shared polling runtime over a transport adapter and backend event handler."""

    def __init__(
        self,
        *,
        core: CoreT,
        access_point: AccessPointT,
        backend_driver: DriverT,
        transport: ProxyTransportAdapter,
        status_event_type: type[StatusEventT],
        handle_backend_event: Callable[[object], None],
    ) -> None:
        super().__init__(core=core, access_point=access_point, backend_driver=backend_driver)
        self._transport = transport
        self._status_event_type = status_event_type
        self._handle_backend_event = handle_backend_event

    def run(
        self,
        *,
        max_poll_cycles: int | None,
        request_timeout_sec: float,
        sleep_sec: float,
        sleep_after_progress: bool,
    ) -> int:
        try:
            return self._run_polling_loop(
                max_poll_cycles=max_poll_cycles,
                request_timeout_sec=request_timeout_sec,
                sleep_sec=sleep_sec,
                sleep_after_progress=sleep_after_progress,
                before_run=self._transport.start,
                after_run=self._transport.stop,
            )
        except StopProxyRuntime as exc:
            return exc.rc

    def _drain_driver_once(self) -> bool:
        return self._drain_backend_events(
            status_event_type=self._status_event_type,
            handle_event=self._handle_backend_event,
        )

    def _consume_transport_once(self) -> bool:
        return self._transport.consume_once()

    def _transport_ready(self) -> bool:
        return bool(self._transport.ready())
