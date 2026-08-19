"""Typed request/response contract for approval delegation between agents."""

from __future__ import annotations

from dataclasses import dataclass
import json

from orchestrator.approval import ApprovalRequest
from orchestrator.approval_details_formatter import build_approval_request_param_lines


@dataclass(frozen=True)
class ApprovalDelegationDecision:
    approval_id: str
    decision: str


def build_approval_delegation_prompt(
    *,
    approval_id: str,
    source_label: str,
    target_address: str,
    request: ApprovalRequest,
) -> str:
    request_json = json.dumps(request.params, ensure_ascii=True, sort_keys=True, indent=2)
    return (
        "ORC typed approval delegation request.\n"
        "Do not run tools. Decide only from the supplied request.\n"
        "Return exactly one JSON object and no other text:\n"
        f'{{"type":"approval_decision","approval_id":"{approval_id}",'
        '"decision":"accept|decline"}\n\n'
        f"source: {source_label}\n"
        f"target_address: {target_address}\n"
        f"method: {request.method}\n"
        f"request:\n{request_json}"
    )


def parse_approval_delegation_decision(
    text: str,
    *,
    expected_approval_id: str,
) -> ApprovalDelegationDecision:
    try:
        payload = json.loads(str(text or "").strip())
    except json.JSONDecodeError as exc:
        raise ValueError(f"approval decision must be one JSON object: {exc.msg}") from exc
    if not isinstance(payload, dict):
        raise ValueError("approval decision must be a JSON object")
    if set(payload) != {"type", "approval_id", "decision"}:
        raise ValueError("approval decision must contain exactly type, approval_id, decision")
    if payload.get("type") != "approval_decision":
        raise ValueError("approval decision type must be approval_decision")
    approval_id = str(payload.get("approval_id") or "")
    if approval_id != expected_approval_id:
        raise ValueError(
            f"approval decision id mismatch: expected {expected_approval_id}, got {approval_id or '<empty>'}"
        )
    decision = str(payload.get("decision") or "").strip().lower()
    if decision not in {"accept", "decline"}:
        raise ValueError("approval decision must be accept or decline")
    return ApprovalDelegationDecision(approval_id=approval_id, decision=decision)


def format_approval_request_copy(
    *,
    source_label: str,
    target_address: str,
    request: ApprovalRequest,
) -> str:
    lines = [
        "approval request",
        f"from: {source_label}",
        f"to: {target_address}",
        f"method: {request.method}",
    ]
    lines.extend(build_approval_request_param_lines(request))
    return "\n".join(lines)
