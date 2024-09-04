from enum import Enum


class PeriodType(str, Enum):
    Annual = "annual"
    """
    Annual
    """
    Quarterly = "quarterly"
    """
    Quarterly
    """
    Combined = "combined"
    """
    Annual and quarterly
    """
    TrailingTwelveMonths = "TTM"
    """
    Trailing twelve months
    """
