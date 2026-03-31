"""Shared runner/bootstrap helpers for proxy-style runtimes."""

from __future__ import annotations

from typing import Any, Callable, Generic, Hashable, TypeVar

from orchestrator.processes import LifecycleLogger
from orchestrator.proxy_runtime import ProxyAgentRuntime, ProxyTransportAdapter


AccessPointKeyT = TypeVar("AccessPointKeyT", bound=Hashable)
CoreT = TypeVar("CoreT")
AccessPointT = TypeVar("AccessPointT")
DriverT = TypeVar("DriverT")
StatusEventT = TypeVar("StatusEventT")
Writer = Callable[[str], None]


def build_proxy_runtime(
    *,
    core: CoreT,
    access_point: AccessPointT,
    backend_driver: DriverT,
    transport_factory: Callable[[Callable[[object], None]], ProxyTransportAdapter],
    status_event_type: type[StatusEventT],
    handle_backend_event: Callable[[object], None],
) -> ProxyAgentRuntime[AccessPointKeyT, CoreT, AccessPointT, DriverT, StatusEventT]:
    runtime_ref: dict[str, Any] = {}
    transport = transport_factory(lambda action: runtime_ref["runtime"]._dispatch_proxy_action(action))
    runtime = ProxyAgentRuntime[AccessPointKeyT, CoreT, AccessPointT, DriverT, StatusEventT](
        core=core,
        access_point=access_point,
        backend_driver=backend_driver,
        transport=transport,
        status_event_type=status_event_type,
        handle_backend_event=handle_backend_event,
    )
    runtime_ref["runtime"] = runtime
    return runtime


def run_proxy_runtime(
    *,
    runtime: ProxyAgentRuntime[Any, Any, Any, Any, Any],
    backend_driver: Any,
    request_timeout_sec: float,
    max_poll_cycles: int | None,
    sleep_sec: float,
    sleep_after_progress: bool,
    writer: Writer,
    logger: LifecycleLogger,
    stopping_message: str,
    sigint_event_name: str,
    stopped_event_name: str,
    stop_resources: Callable[[], None],
    handle_error: Callable[[Exception], int],
) -> int:
    backend_driver.start()
    try:
        return runtime.run(
            request_timeout_sec=request_timeout_sec,
            max_poll_cycles=max_poll_cycles,
            sleep_sec=sleep_sec,
            sleep_after_progress=sleep_after_progress,
        )
    except KeyboardInterrupt:
        writer("^C")
        writer(stopping_message)
        logger.event(sigint_event_name)
        return 130
    except Exception as exc:
        return handle_error(exc)
    finally:
        backend_driver.stop()
        stop_resources()
        logger.event(stopped_event_name)
