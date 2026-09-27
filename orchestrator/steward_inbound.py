"""Transport-normalized inbound shapes for steward runtime."""

from __future__ import annotations

from dataclasses import dataclass

from orchestrator.access_point_common import (
    AccessPointKey,
    AccessPointApprovalDecision,
    AccessPointApprovalDetailsRequest,
    AccessPointTextInput,
)
from orchestrator.uploads import PendingUpload


@dataclass(frozen=True)
class StewardInboundText(AccessPointTextInput[AccessPointKey]):
    uploads: tuple[PendingUpload, ...] = ()


class StewardApprovalDecision(AccessPointApprovalDecision[AccessPointKey]):
    pass


class StewardApprovalDetailsRequest(AccessPointApprovalDetailsRequest[AccessPointKey]):
    pass
