from typing import Optional, Sequence
from pydantic import BaseModel


class StandardizedParameters(BaseModel):
    """
    Cooresponds to StandardizedQueryParameters.cs on the server
    """

    metrics: Optional[Sequence[str]] = None
    includeTraceV2: Optional[bool] = False
    """
    added september 2024 in version 14.1.1

    We had includeTrace set to true in the standardized function and we added tracing to the PIT end-point so everybody was getting the tracing.
    """
    pointInTime: Optional[bool] = False
    allFootnotes: Optional[bool] = False
    allFace: Optional[bool] = False
    allNonGAAP: Optional[bool] = False
    allMetrics: Optional[bool] = False
    pointInTimeV2: Optional[bool] = False
    includePreliminary: Optional[bool] = False
    """
    only applies to PIT V1
    """
    XBRLOnly: Optional[bool] = False
