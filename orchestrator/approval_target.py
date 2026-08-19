"""Validation helpers for persisted per-binding approval delegation targets."""

from __future__ import annotations

from collections.abc import Hashable, Mapping
from typing import TypeVar

from orchestrator.binding_address import normalize_address


BindingKeyT = TypeVar("BindingKeyT", bound=Hashable)
HUMAN_APPROVAL_TARGET = "human"


def normalize_approval_target(raw: object) -> str:
    if isinstance(raw, str) and raw.strip().lower() == HUMAN_APPROVAL_TARGET:
        return HUMAN_APPROVAL_TARGET
    return normalize_address(raw)


def validate_approval_target_assignment(
    *,
    source: BindingKeyT,
    raw_target: object,
    addresses: Mapping[BindingKeyT, str],
    targets: Mapping[BindingKeyT, str],
) -> str:
    target = normalize_approval_target(raw_target)
    if target == HUMAN_APPROVAL_TARGET:
        return target
    source_address = addresses.get(source) or "<unaddressed>"
    binding_by_address = {address: binding for binding, address in addresses.items()}
    target_binding = binding_by_address.get(target)
    if target_binding is None:
        raise ValueError(f"approval target address is not assigned: {target}")
    if target_binding == source:
        raise ValueError(f"approval target cannot reference its own address: {target}")

    candidate = dict(targets)
    candidate[source] = target
    path = [source_address]
    cursor = source
    visited: set[BindingKeyT] = set()
    while cursor not in visited:
        visited.add(cursor)
        next_target = normalize_approval_target(candidate.get(cursor, HUMAN_APPROVAL_TARGET))
        if next_target == HUMAN_APPROVAL_TARGET:
            return target
        path.append(next_target)
        next_binding = binding_by_address.get(next_target)
        if next_binding is None:
            raise ValueError(f"approval target address is not assigned: {next_target}")
        if next_binding == source:
            raise ValueError(f"approval target cycle: {' -> '.join(path)}")
        cursor = next_binding
    return target


def approval_target_referrers(
    *,
    target_address: str,
    addresses: Mapping[BindingKeyT, str],
    targets: Mapping[BindingKeyT, str],
) -> list[str]:
    return sorted(
        addresses.get(binding) or "<unaddressed>"
        for binding, approval_target in targets.items()
        if normalize_approval_target(approval_target) == target_address
    )


def sanitize_approval_targets(
    *,
    addresses: Mapping[BindingKeyT, str],
    targets: Mapping[BindingKeyT, object],
) -> tuple[dict[BindingKeyT, str], dict[BindingKeyT, str]]:
    canonical: dict[BindingKeyT, str] = {}
    errors: dict[BindingKeyT, str] = {}
    for binding, raw_target in targets.items():
        try:
            canonical[binding] = normalize_approval_target(raw_target)
        except ValueError as exc:
            errors[binding] = str(exc)

    for binding in list(canonical):
        if binding in errors:
            continue
        try:
            validate_approval_target_assignment(
                source=binding,
                raw_target=canonical[binding],
                addresses=addresses,
                targets=canonical,
            )
        except ValueError as exc:
            errors[binding] = str(exc)

    sanitized = {
        binding: (
            HUMAN_APPROVAL_TARGET
            if binding in errors
            else canonical.get(binding, HUMAN_APPROVAL_TARGET)
        )
        for binding in targets
    }
    return sanitized, errors
