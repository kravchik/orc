from __future__ import annotations

import re
from collections.abc import Hashable, Mapping
from dataclasses import dataclass
from typing import Generic, TypeVar


BindingKeyT = TypeVar("BindingKeyT", bound=Hashable)

_ADDRESS_RE = re.compile(r"[a-z][a-z0-9_-]{0,63}\Z")
_RESERVED_ADDRESSES = frozenset({"human"})
ROUTING_MODE_SHAMAN = "shaman"
ROUTING_MODE_GRUNT = "grunt"
ROUTING_MODES = frozenset({ROUTING_MODE_SHAMAN, ROUTING_MODE_GRUNT})


@dataclass(frozen=True)
class RoutingIdentity:
    address: str
    mode: str


@dataclass(frozen=True)
class ResolvedRoutingIdentity(Generic[BindingKeyT]):
    binding: BindingKeyT
    address: str
    mode: str


def normalize_address(raw: object) -> str:
    if not isinstance(raw, str):
        raise ValueError("address must be a string")
    address = raw.strip().lower()
    if not address:
        raise ValueError("address is required")
    if address in _RESERVED_ADDRESSES:
        raise ValueError(f"address is reserved: {address}")
    if _ADDRESS_RE.fullmatch(address) is None:
        raise ValueError("address must match [a-z][a-z0-9_-]{0,63}")
    return address


def normalize_routing_mode(raw: object) -> str:
    if not isinstance(raw, str):
        raise ValueError("routing mode must be a string")
    mode = raw.strip().lower()
    if mode not in ROUTING_MODES:
        raise ValueError(f"routing mode must be one of: {', '.join(sorted(ROUTING_MODES))}")
    return mode


class RoutingIdentityRegistry(Generic[BindingKeyT]):
    def __init__(
        self,
        initial: Mapping[BindingKeyT, str] | None = None,
        *,
        modes: Mapping[BindingKeyT, str] | None = None,
    ) -> None:
        self._identity_by_binding: dict[BindingKeyT, RoutingIdentity] = {}
        self._binding_by_address: dict[str, BindingKeyT] = {}
        if initial:
            self.replace_all(initial, modes=modes)
        elif modes:
            raise ValueError("routing mode requires an address")

    def assign(
        self,
        binding: BindingKeyT,
        raw_address: object,
        raw_mode: object,
    ) -> RoutingIdentity:
        address = normalize_address(raw_address)
        mode = normalize_routing_mode(raw_mode)
        current = self._identity_by_binding.get(binding)
        if current is not None and current.mode == mode:
            raise ValueError(f"binding already has address: {current.address}")
        owner = self._binding_by_address.get(address)
        if owner is not None and owner != binding:
            raise ValueError(f"address is already assigned: {address}")
        if current is not None and current.address != address:
            del self._binding_by_address[current.address]
        identity = RoutingIdentity(address=address, mode=mode)
        self._identity_by_binding[binding] = identity
        self._binding_by_address[address] = binding
        return identity

    def rename(
        self,
        binding: BindingKeyT,
        raw_address: object,
        raw_mode: object,
    ) -> RoutingIdentity:
        mode = normalize_routing_mode(raw_mode)
        current = self._identity_by_binding.get(binding)
        if current is None or current.mode != mode:
            raise ValueError("binding has no address")
        address = normalize_address(raw_address)
        if address == current.address:
            return current
        owner = self._binding_by_address.get(address)
        if owner is not None and owner != binding:
            raise ValueError(f"address is already assigned: {address}")
        del self._binding_by_address[current.address]
        renamed = RoutingIdentity(
            address=address,
            mode=current.mode,
        )
        self._identity_by_binding[binding] = renamed
        self._binding_by_address[address] = binding
        return renamed

    def remove(self, binding: BindingKeyT, raw_mode: object) -> RoutingIdentity:
        mode = normalize_routing_mode(raw_mode)
        identity = self._identity_by_binding.get(binding)
        if identity is None or identity.mode != mode:
            raise ValueError("binding has no address")
        del self._identity_by_binding[binding]
        del self._binding_by_address[identity.address]
        return identity

    def identity(self, binding: BindingKeyT) -> RoutingIdentity | None:
        return self._identity_by_binding.get(binding)

    def items(self) -> tuple[tuple[BindingKeyT, str], ...]:
        return tuple(
            (binding, identity.address)
            for binding, identity in self._identity_by_binding.items()
        )

    def resolve(
        self,
        raw_address: object,
    ) -> ResolvedRoutingIdentity[BindingKeyT] | None:
        address = normalize_address(raw_address)
        binding = self._binding_by_address.get(address)
        if binding is None:
            return None
        identity = self._identity_by_binding[binding]
        return ResolvedRoutingIdentity(
            binding=binding,
            address=identity.address,
            mode=identity.mode,
        )

    def replace_all(
        self,
        values: Mapping[BindingKeyT, str],
        *,
        modes: Mapping[BindingKeyT, str] | None = None,
    ) -> None:
        if modes and any(binding not in values for binding in modes):
            raise ValueError("routing mode requires an address")
        candidate = RoutingIdentityRegistry[BindingKeyT]()
        for binding, address in values.items():
            mode = modes.get(binding, ROUTING_MODE_SHAMAN) if modes else ROUTING_MODE_SHAMAN
            candidate.assign(binding, address, mode)
        self._identity_by_binding = dict(candidate._identity_by_binding)
        self._binding_by_address = dict(candidate._binding_by_address)
