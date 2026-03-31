"""Node/Hub contracts and in-memory runtime manager."""

from __future__ import annotations

import threading
import uuid
from dataclasses import dataclass
from typing import Callable, Protocol


NodeEventCallback = Callable[[str, dict], None]
StatusCallback = Callable[[str], None]
HubListener = Callable[["HubEvent"], None]


class Node(Protocol):
    """Single-agent runtime contract."""

    def start(self) -> None: ...

    def stop(self) -> None: ...

    def send(
        self,
        text: str,
        *,
        status_update: StatusCallback | None = None,
        approval_notify: Callable[[str], None] | None = None,
    ) -> str: ...

    def set_event_callback(self, callback: NodeEventCallback | None) -> None: ...


@dataclass(frozen=True)
class HubEvent:
    node_id: str
    event: str
    payload: dict


@dataclass(frozen=True)
class HubNodeStatus:
    node_id: str
    state: str


@dataclass
class _NodeEntry:
    node: Node
    state: str
    listeners: list[HubListener]


class InMemoryHub:
    """Simple in-process Node manager."""

    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._nodes: dict[str, _NodeEntry] = {}

    def create(self, node: Node, *, node_id: str | None = None) -> str:
        resolved = node_id or uuid.uuid4().hex
        with self._lock:
            if resolved in self._nodes:
                raise RuntimeError(f"node id already exists: {resolved}")
            entry = _NodeEntry(node=node, state="created", listeners=[])
            self._nodes[resolved] = entry
            node.set_event_callback(lambda event, payload: self._emit(resolved, event, payload))
            self._emit(resolved, "node_created", {})
            return resolved

    def start(self, node_id: str) -> None:
        entry = self._require(node_id)
        if entry.state == "running":
            return
        entry.node.start()
        with self._lock:
            entry.state = "running"
        self._emit(node_id, "node_started", {})

    def stop(self, node_id: str) -> None:
        entry = self._require(node_id)
        if entry.state == "stopped":
            return
        entry.node.stop()
        with self._lock:
            entry.state = "stopped"
        self._emit(node_id, "node_stopped", {})

    def send(
        self,
        node_id: str,
        text: str,
        *,
        status_update: StatusCallback | None = None,
        approval_notify: Callable[[str], None] | None = None,
    ) -> str:
        entry = self._require(node_id)
        if entry.state != "running":
            raise RuntimeError(f"node is not running: {node_id}")
        self._emit(node_id, "node_input", {"text": text})
        reply = entry.node.send(text, status_update=status_update, approval_notify=approval_notify)
        self._emit(node_id, "node_output", {"text": reply})
        return reply

    def status(self, node_id: str) -> HubNodeStatus:
        entry = self._require(node_id)
        return HubNodeStatus(node_id=node_id, state=entry.state)

    def get_node(self, node_id: str) -> Node:
        entry = self._require(node_id)
        return entry.node

    def subscribe(self, node_id: str, listener: HubListener) -> Callable[[], None]:
        entry = self._require(node_id)
        with self._lock:
            entry.listeners.append(listener)

        def _unsubscribe() -> None:
            with self._lock:
                try:
                    entry.listeners.remove(listener)
                except ValueError:
                    return

        return _unsubscribe

    def _emit(self, node_id: str, event: str, payload: dict) -> None:
        with self._lock:
            entry = self._nodes.get(node_id)
            if entry is None:
                return
            listeners = list(entry.listeners)
        hub_event = HubEvent(node_id=node_id, event=event, payload=dict(payload))
        for listener in listeners:
            listener(hub_event)

    def _require(self, node_id: str) -> _NodeEntry:
        with self._lock:
            entry = self._nodes.get(node_id)
        if entry is None:
            raise RuntimeError(f"unknown node id: {node_id}")
        return entry
