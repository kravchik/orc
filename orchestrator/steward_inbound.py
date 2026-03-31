"""Transport-normalized inbound shapes for steward runtime."""

from __future__ import annotations

from orchestrator.access_point_common import (
    AccessPointApprovalDecision,
    AccessPointApprovalDetailsRequest,
    AccessPointTextInput,
)
from orchestrator.telegram_steward_helpers import AccessPointKey


class StewardInboundText(AccessPointTextInput[AccessPointKey]):
    pass


class StewardApprovalDecision(AccessPointApprovalDecision[AccessPointKey]):
    pass


class StewardApprovalDetailsRequest(AccessPointApprovalDetailsRequest[AccessPointKey]):
    pass
