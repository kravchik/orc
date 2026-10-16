"""Projection-owned configuration choices shared by all interactive surfaces."""

from __future__ import annotations

from dataclasses import dataclass

from orchestrator.agent_capabilities import AgentCapability
from orchestrator.agent_inspection import AgentRuntimeProjection, SnapshotAvailability


MODEL_PICKER_REQUIRED_CAPABILITIES: tuple[AgentCapability, ...] = (
    AgentCapability.LIST_MODELS,
    AgentCapability.SET_MODEL,
)

@dataclass(frozen=True)
class ConfigurationChoiceView:
    kind: str
    current: str
    values: tuple[str, ...]
    availability: SnapshotAvailability

    @property
    def available(self) -> bool:
        return self.availability is SnapshotAvailability.AVAILABLE and bool(self.values)


def configuration_choice_view(
    projection: AgentRuntimeProjection,
    *,
    kind: str,
) -> ConfigurationChoiceView:
    configuration = projection.configuration
    if kind == "model":
        current = configuration.current_model
        values = configuration.models
    elif kind == "effort":
        current = configuration.current_effort
        values = configuration.effort_values
    else:
        raise ValueError("configuration choice kind must be model or effort")
    return ConfigurationChoiceView(
        kind=kind,
        current=current,
        values=values,
        availability=configuration.availability,
    )


def format_configuration_choices(view: ConfigurationChoiceView) -> str:
    label = view.kind
    if not view.available:
        state = (
            "unavailable"
            if view.availability is SnapshotAvailability.AVAILABLE
            else view.availability.value
        )
        return f"{label} choices are {state}"
    lines = [f"Select {label} for this access point."]
    if view.current:
        lines.append(f"Current: {view.current}")
    lines.extend(
        (
            f"Use `/{label} <name>`.",
            f"Options: {', '.join(view.values)}",
        )
    )
    return "\n".join(lines)


def format_effort_inspect_lines(projection: AgentRuntimeProjection) -> tuple[str, ...]:
    view = configuration_choice_view(projection, kind="effort")
    if view.availability is SnapshotAvailability.AVAILABLE and view.values:
        return (
            f"effort: {view.current or 'unknown'}",
            f"effort options: {', '.join(view.values)}",
        )
    state = (
        "unavailable"
        if view.availability is SnapshotAvailability.AVAILABLE
        else view.availability.value
    )
    return (f"effort: {state}",)
