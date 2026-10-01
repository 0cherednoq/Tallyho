"""Reference S1/S2/S3 application used by the acceptance stand."""

from __future__ import annotations

from tests.acceptance.app.common import FaultKind, FaultPlan, PermanentError, TransientError
from tests.acceptance.app.site import CatalogGenerator, CatalogTruth, FakeCatalogSite

__all__ = [
    "CatalogGenerator",
    "CatalogTruth",
    "FakeCatalogSite",
    "FaultKind",
    "FaultPlan",
    "PermanentError",
    "TransientError",
]
