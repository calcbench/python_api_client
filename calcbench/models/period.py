from enum import IntEnum
from typing import Literal, Optional, Union


class Period(IntEnum):
    """
    Fiscal period.
    """

    Annual = 0
    Q1 = 1
    Q2 = 2
    Q3 = 3
    Q4 = 4
    H1 = 5
    """
    First half of year
    """
    Q3Cum = 6
    """
    First three quarters of year
    """
    Other = 9
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
