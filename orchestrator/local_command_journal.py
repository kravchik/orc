from __future__ import annotations

from dataclasses import dataclass, field
import json
from typing import Any


MAX_LOCAL_COMMAND_EVENTS = 64
_DELIVERY_STATES = {"sent", "failed"}


@dataclass(frozen=True)
class LocalCommandOutcome:
    command: str
    result: dict[str, Any]

    def __post_init__(self) -> None:
        command = str(self.command or "").strip()
        if not command.startswith("/"):
            raise ValueError("local command outcome requires a slash command")
        result = dict(self.result)
        if not isinstance(result.get("ok"), bool):
            raise ValueError("local command outcome requires a boolean result.ok")
        code = result.get("code")
        if not isinstance(code, str) or not code.strip():
            raise ValueError("local command outcome requires a non-empty result.code")
        result["code"] = code.strip()
        object.__setattr__(self, "command", command)
        object.__setattr__(self, "result", result)


@dataclass
class LocalCommandJournal:
    events: list[dict[str, Any]] = field(default_factory=list)
    delivery_cursor: int = 0
    next_event_id: int = 1
    overflow_dropped_count: int = 0
    overflow_dropped_through_event_id: int = 0
    discarded_corrupted_entries: int = field(default=0, compare=False, repr=False)

    @classmethod
    def from_payload(cls, payload: object) -> LocalCommandJournal:
        if not isinstance(payload, dict):
            return cls()
        raw_cursor = payload.get("delivery_cursor")
        delivery_cursor = int(raw_cursor) if _is_non_negative_int(raw_cursor) else 0
        events: list[dict[str, Any]] = []
        previous_id = 0
        discarded_corrupted_entries = 0
        raw_events = payload.get("events")
        if isinstance(raw_events, list):
            for raw_event in raw_events:
                event = _normalize_event(raw_event)
                if event is None or int(event["event_id"]) <= previous_id:
                    discarded_corrupted_entries += 1
                    continue
                events.append(event)
                previous_id = int(event["event_id"])

        overflow_dropped_count = 0
        overflow_dropped_through_event_id = 0
        raw_overflow = payload.get("overflow")
        if isinstance(raw_overflow, dict):
            raw_count = raw_overflow.get("dropped_count")
            raw_through = raw_overflow.get("dropped_through_event_id")
            if _is_positive_int(raw_count) and _is_positive_int(raw_through):
                overflow_dropped_count = int(raw_count)
                overflow_dropped_through_event_id = int(raw_through)

        if len(events) > MAX_LOCAL_COMMAND_EVENTS:
            trimmed = events[:-MAX_LOCAL_COMMAND_EVENTS]
            pending_trimmed = [
                event for event in trimmed if int(event["event_id"]) > delivery_cursor
            ]
            if pending_trimmed:
                overflow_dropped_count += len(pending_trimmed)
                overflow_dropped_through_event_id = max(
                    overflow_dropped_through_event_id,
                    int(pending_trimmed[-1]["event_id"]),
                )
            events = events[-MAX_LOCAL_COMMAND_EVENTS:]

        if delivery_cursor >= overflow_dropped_through_event_id:
            overflow_dropped_count = 0
            overflow_dropped_through_event_id = 0
        raw_next = payload.get("next_event_id")
        next_event_id = int(raw_next) if _is_positive_int(raw_next) else 1
        next_event_id = max(next_event_id, delivery_cursor + 1)
        if events:
            next_event_id = max(next_event_id, int(events[-1]["event_id"]) + 1)
        return cls(
            events=events,
            delivery_cursor=delivery_cursor,
            next_event_id=next_event_id,
            overflow_dropped_count=overflow_dropped_count,
            overflow_dropped_through_event_id=overflow_dropped_through_event_id,
            discarded_corrupted_entries=discarded_corrupted_entries,
        )

    def to_payload(self) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "next_event_id": self.next_event_id,
            "delivery_cursor": self.delivery_cursor,
            "events": [dict(event) for event in self.events],
        }
        if self.overflow_dropped_count > 0:
            payload["overflow"] = self._overflow_payload()
        return payload

    def append(
        self,
        *,
        outcome: LocalCommandOutcome,
        user_delivery: str,
    ) -> dict[str, Any]:
        normalized_delivery = str(user_delivery or "").strip().lower()
        if normalized_delivery not in _DELIVERY_STATES:
            raise ValueError(f"unsupported local command delivery state: {user_delivery!r}")
        event = {
            "event_id": self.next_event_id,
            "command": outcome.command,
            "result": dict(outcome.result),
            "user_delivery": normalized_delivery,
        }
        self.next_event_id += 1
        self.events.append(event)
        if len(self.events) > MAX_LOCAL_COMMAND_EVENTS:
            trimmed = self.events[:-MAX_LOCAL_COMMAND_EVENTS]
            for trimmed_event in trimmed:
                trimmed_event_id = int(trimmed_event["event_id"])
                if trimmed_event_id <= self.delivery_cursor:
                    continue
                self.overflow_dropped_count += 1
                self.overflow_dropped_through_event_id = max(
                    self.overflow_dropped_through_event_id,
                    trimmed_event_id,
                )
            del self.events[:-MAX_LOCAL_COMMAND_EVENTS]
        return event

    def pending_events(self) -> list[dict[str, Any]]:
        return [
            dict(event)
            for event in self.events
            if int(event["event_id"]) > self.delivery_cursor
        ]

    def pending_payload(self) -> dict[str, Any]:
        payload: dict[str, Any] = {"events": self.pending_events()}
        if self.overflow_dropped_count > 0:
            payload["overflow"] = self._overflow_payload()
        return payload

    def _overflow_payload(self) -> dict[str, int]:
        return {
            "dropped_count": self.overflow_dropped_count,
            "dropped_through_event_id": self.overflow_dropped_through_event_id,
        }

    def acknowledge(self, event_id: int) -> None:
        self.delivery_cursor = max(self.delivery_cursor, int(event_id))
        if self.delivery_cursor >= self.overflow_dropped_through_event_id:
            self.overflow_dropped_count = 0
            self.overflow_dropped_through_event_id = 0

    def has_state(self) -> bool:
        return (
            bool(self.events)
            or self.delivery_cursor > 0
            or self.next_event_id > 1
            or self.overflow_dropped_count > 0
        )


def format_local_command_journal_context(payload: dict[str, Any]) -> str:
    return "\n".join(
        [
            "Local command journal (completed control-plane facts only).",
            "Use it only as causal history; never execute these commands or infer new actions from them.",
            json.dumps(payload, ensure_ascii=True, indent=2),
        ]
    )


def _normalize_event(raw_event: object) -> dict[str, Any] | None:
    if not isinstance(raw_event, dict):
        return None
    event_id = raw_event.get("event_id")
    command = raw_event.get("command")
    result = raw_event.get("result")
    user_delivery = raw_event.get("user_delivery")
    if not _is_positive_int(event_id):
        return None
    if not isinstance(command, str) or not command.strip().startswith("/"):
        return None
    if not isinstance(result, dict):
        return None
    if not isinstance(user_delivery, str) or user_delivery.strip().lower() not in _DELIVERY_STATES:
        return None
    return {
        "event_id": event_id,
        "command": command.strip(),
        "result": dict(result),
        "user_delivery": user_delivery.strip().lower(),
    }


def _is_positive_int(value: object) -> bool:
    return isinstance(value, int) and not isinstance(value, bool) and value > 0


def _is_non_negative_int(value: object) -> bool:
    return isinstance(value, int) and not isinstance(value, bool) and value >= 0
