"""Transport-normalized inbound shapes for steward runtime."""

from __future__ import annotations

from orchestrator.access_point_common import (
    AccessPointKey,
    AccessPointApprovalDecision,
    AccessPointApprovalDetailsRequest,
    AccessPointTextInput,
)


class StewardInboundText(AccessPointTextInput[AccessPointKey]):
    pass


class StewardApprovalDecision(AccessPointApprovalDecision[AccessPointKey]):
    pass


class StewardApprovalDetailsRequest(AccessPointApprovalDetailsRequest[AccessPointKey]):
    pass
