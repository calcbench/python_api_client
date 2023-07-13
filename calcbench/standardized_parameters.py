from typing import Optional, Sequence
from pydantic import BaseModel


class StandardizedParameters(BaseModel):
    """
    Cooresponds to MappedDataSearchParameters.cs on the server
    """

    metrics: Optional[Sequence[str]] = None
    includeTrace: Optional[bool] = False
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
