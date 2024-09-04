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
    Annual and quarter data
    """
    TrailingTwelveMonths = "TTM"
    """
    Trailing twelve months
    """
