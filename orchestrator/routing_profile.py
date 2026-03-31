"""Routing profile/session contract and migration invariants (Step 20.1)."""

from __future__ import annotations

import re
from dataclasses import dataclass


_SESSION_ID_RE = re.compile(r"^[A-Za-z0-9._:-]+$")
_ROLE_ALIAS_RE = re.compile(r"^@[A-Z][A-Z0-9_]*$")


@dataclass(frozen=True)
class RoutingProfileContract:
    profile_name: str
    required_role_aliases: frozenset[str]


PROFILE_SINGLE_AGENT = "single_agent"
PROFILE_LEAD_WORKER = "lead_worker"


PROFILE_CONTRACTS: dict[str, RoutingProfileContract] = {
    PROFILE_SINGLE_AGENT: RoutingProfileContract(
        profile_name=PROFILE_SINGLE_AGENT,
        required_role_aliases=frozenset({"@AGENT"}),
    ),
    PROFILE_LEAD_WORKER: RoutingProfileContract(
        profile_name=PROFILE_LEAD_WORKER,
        required_role_aliases=frozenset({"@LEAD", "@WORKER"}),
    ),
}


@dataclass(frozen=True)
class RoutingSessionContract:
    session_id: str
    profile: str
    role_alias_to_node_id: dict[str, str]

    def validate(self) -> None:
        validate_session_id(self.session_id)
        contract = PROFILE_CONTRACTS.get(self.profile)
        if contract is None:
            known = ", ".join(sorted(PROFILE_CONTRACTS.keys()))
            raise ValueError(f"unsupported routing profile: {self.profile!r}; expected one of: {known}")
        if not self.role_alias_to_node_id:
            raise ValueError("role_alias_to_node_id must not be empty")
        normalized: dict[str, str] = {}
        for alias_raw, node_id_raw in self.role_alias_to_node_id.items():
            alias = normalize_role_alias(alias_raw)
            node_id = str(node_id_raw or "").strip()
            if not node_id:
                raise ValueError(f"node_id for role alias {alias!r} must be non-empty")
            if alias in normalized:
                raise ValueError(f"duplicate role alias after normalization: {alias!r}")
            normalized[alias] = node_id
        missing = sorted(contract.required_role_aliases.difference(normalized.keys()))
        if missing:
            raise ValueError(
                f"routing profile {self.profile!r} is missing required role aliases: {', '.join(missing)}"
            )


@dataclass(frozen=True)
class AccessPointRouteBinding:
    """Access-point routing invariant.

    Exactly one of:
    - `node_id`: direct model (`access_point -> node`)
    - `session_id`: session model (`access_point -> session/profile`)
    """

    access_point_key: str
    node_id: str = ""
    session_id: str = ""

    def validate(self) -> None:
        key = str(self.access_point_key or "").strip()
        if not key:
            raise ValueError("access_point_key must be non-empty")
        node = str(self.node_id or "").strip()
        session = str(self.session_id or "").strip()
        if bool(node) == bool(session):
            raise ValueError("exactly one of node_id or session_id must be set")
        if session:
            validate_session_id(session)


def normalize_role_alias(alias: str) -> str:
    value = str(alias or "").strip().upper()
    if not value:
        raise ValueError("role alias must be non-empty")
    if not value.startswith("@"):
        value = "@" + value
    if _ROLE_ALIAS_RE.fullmatch(value) is None:
        raise ValueError(f"invalid role alias format: {alias!r}")
    return value


def validate_session_id(session_id: str) -> None:
    value = str(session_id or "").strip()
    if not value:
        raise ValueError("session_id must be non-empty")
    if _SESSION_ID_RE.fullmatch(value) is None:
        raise ValueError(
            "invalid session_id format: expected [A-Za-z0-9._:-]+"
        )


def build_single_agent_session_contract(
    *,
    session_id: str,
    node_id: str,
    agent_alias: str = "@AGENT",
) -> RoutingSessionContract:
    alias = normalize_role_alias(agent_alias)
    node = str(node_id or "").strip()
    if not node:
        raise ValueError("node_id must be non-empty")
    session = RoutingSessionContract(
        session_id=session_id,
        profile=PROFILE_SINGLE_AGENT,
        role_alias_to_node_id={alias: node},
    )
    session.validate()
    return session
