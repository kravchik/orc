"""Tick-owned queue shared by control-plane interactions."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Generic, Protocol, TypeVar

from orchestrator.access_point_common import AccessPointKey


@dataclass
class InteractionTickContext:
    runtime: Any
    blocked_targets: set[AccessPointKey] = field(default_factory=set)


class Interaction(Protocol):
    def tick(self, context: Any) -> str | None: ...

    def on_completed(self, context: Any, outcome: str) -> None: ...


ItemT = TypeVar("ItemT", bound=Interaction)


class InteractionQueue(Generic[ItemT]):
    def __init__(self) -> None:
        self._items: list[ItemT] = []
        self._next_sequence = 1

    def __bool__(self) -> bool:
        return bool(self._items)

    def next_sequence(self) -> int:
        sequence = self._next_sequence
        self._next_sequence += 1
        return sequence

    def append(self, interaction: ItemT) -> None:
        self._items.append(interaction)

    def snapshot(self) -> tuple[ItemT, ...]:
        return tuple(self._items)

    def tick(self, context: object) -> bool:
        progressed = False
        for interaction in self.snapshot():
            outcome = interaction.tick(context)
            if outcome is None:
                continue
            for index, item in enumerate(self._items):
                if item is interaction:
                    del self._items[index]
                    break
            interaction.on_completed(context, outcome)
            progressed = True
        return progressed
