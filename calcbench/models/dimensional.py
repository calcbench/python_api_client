from typing import Dict, Optional
from calcbench.models.standardized import StandardizedPoint


class DimensionalDataPoint(StandardizedPoint):
    """
    The data returned by calls to the dimensional api end-point

    """

    container: str
    """
    Deprecated.  Use the label instead.
    """
    dimensions: Dict[str, str]
    """
    Deprecated.  Use the label instead.
    """
    label: str
    """
    Human readable label for the line.  Use this instead of dimensions.
    """

    standardized_label: Optional[str] = None
    """
    Calcbench's effort to standardize the label.  Only work for geographic segments as of April 2024
    """
