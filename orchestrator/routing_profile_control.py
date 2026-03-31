"""In-memory control-plane registry for routing profiles and attachments."""

from __future__ import annotations

import threading
from dataclasses import dataclass

from orchestrator.routing_profile import RoutingSessionContract, normalize_role_alias


@dataclass(frozen=True)
class RoutingAttachment:
    access_point_key: str
    session_id: str
    source_alias: str


class InMemoryRoutingProfileControl:
    """Simple runtime registry for profile management control-path."""

    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._sessions: dict[str, RoutingSessionContract] = {}
        self._attachments: dict[str, RoutingAttachment] = {}

    def list_profiles(self) -> list[dict]:
        with self._lock:
            sessions = list(self._sessions.values())
        result: list[dict] = []
        for session in sorted(sessions, key=lambda item: item.session_id):
            result.append(
                {
                    "session_id": session.session_id,
                    "profile": session.profile,
                    "roles": dict(session.role_alias_to_node_id),
                }
            )
        return result

    def get_profile(self, session_id: str) -> dict | None:
        sid = str(session_id or "").strip()
        with self._lock:
            session = self._sessions.get(sid)
        if session is None:
            return None
        return {
            "session_id": session.session_id,
            "profile": session.profile,
            "roles": dict(session.role_alias_to_node_id),
        }

    def set_profile(
        self,
        *,
        session_id: str,
        profile: str,
        roles: dict[str, str],
    ) -> dict:
        contract = RoutingSessionContract(
            session_id=str(session_id or "").strip(),
            profile=str(profile or "").strip(),
            role_alias_to_node_id={str(k): str(v) for k, v in dict(roles).items()},
        )
        contract.validate()
        with self._lock:
            self._sessions[contract.session_id] = contract
        return {
            "session_id": contract.session_id,
            "profile": contract.profile,
            "roles": dict(contract.role_alias_to_node_id),
        }

    def attach(
        self,
        *,
        access_point_key: str,
        session_id: str,
        source_alias: str,
    ) -> dict:
        ap = str(access_point_key or "").strip()
        if not ap:
            raise ValueError("access_point_key must be non-empty")
        sid = str(session_id or "").strip()
        if not sid:
            raise ValueError("session_id must be non-empty")
        source = normalize_role_alias(source_alias)
        with self._lock:
            if sid not in self._sessions:
                raise RuntimeError(f"unknown session id: {sid}")
            attachment = RoutingAttachment(
                access_point_key=ap,
                session_id=sid,
                source_alias=source,
            )
            self._attachments[ap] = attachment
        return {
            "access_point_key": attachment.access_point_key,
            "session_id": attachment.session_id,
            "source_alias": attachment.source_alias,
        }

    def detach(self, *, access_point_key: str) -> dict:
        ap = str(access_point_key or "").strip()
        if not ap:
            raise ValueError("access_point_key must be non-empty")
        with self._lock:
            removed = self._attachments.pop(ap, None)
        if removed is None:
            raise RuntimeError(f"access point is not attached: {ap}")
        return {
            "access_point_key": removed.access_point_key,
            "session_id": removed.session_id,
            "source_alias": removed.source_alias,
        }
