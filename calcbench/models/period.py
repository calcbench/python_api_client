from enum import IntEnum
from typing import Literal, Optional, Union


class Period(IntEnum):
    """
    Fiscal period.
    """

    Annual = 0
    """
    Annual data
    """
    Q1 = 1
    """
    First quarter data
    """
    Q2 = 2
    """
    Second quarter data
    """
    Q3 = 3
    """
    Third quarter data
    """
    Q4 = 4
    """
    Fourth quarter data
    """
    H1 = 5
    """
    First half of year
    """
    Q3Cum = 6
    """
    First three quarters of year
    """
    Other = 9
    """
    Irregular period, not one of the above, 18 months for example.
    """
    Failure = -1
    """
    Should be few and far between, indicates something went wrong during loading
    """

    @classmethod
    def _missing_(cls, value: object):
        """
        There are other kinds of fiscal periods
        """
        return cls.Other


PeriodArgument = Optional[Union[Period, Literal[0, 1, 2, 3, 4]]]
