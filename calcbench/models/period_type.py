from enum import Enum


class PeriodType(str, Enum):
    Annual = "annual"
    Quarterly = "quarterly"
    Combined = "combined"
    TrailingTwelveMonths = "TTM"
