from datetime import datetime, date
from enum import Enum, IntEnum
from typing import Optional, Sequence, Union

from pydantic import BaseModel


try:
    from typing import Literal
except ImportError:
    from typing_extensions import Literal

CentralIndexKey = Union[str, int]
Ticker = str
CalcbenchCompanyIdentifier = int
CompanyIdentifier = Union[Ticker, CentralIndexKey, CalcbenchCompanyIdentifier]
CompanyIdentifiers = Sequence[CompanyIdentifier]


class CompanyIdentifierScheme(str, Enum):
    Ticker = "ticker"
    CentralIndexKey = "CIK"


class PeriodType(str, Enum):
    Annual = "annual"
    Quarterly = "quarterly"
    Combined = "combined"
    TrailingTwelveMonths = "TTM"


class Period(IntEnum):
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


class CompaniesParameters(BaseModel):
    companyIdentifiers: Optional[CompanyIdentifiers]
    entireUniverse: Optional[bool]


class DateRange(BaseModel):
    startDate: Optional[Union[datetime, date]]
    endDate: Optional[Union[datetime, date]]


class PeriodParameters(BaseModel):
    """
    Corresponds to PeriodParameter.cs
    """

    year: Optional[int] = None
    period: Optional[PeriodArgument] = None
    periodType: Optional[PeriodType] = None
    dateRange: Optional[DateRange] = None
    endYear: Optional[int] = None
    endPeriod: Optional[PeriodArgument] = None
    allHistory: Optional[bool] = None
    updateDate: Optional[date] = None
    useFiscalPeriod: Optional[bool] = None
    accessionID: Optional[int] = None
    filingID: Optional[int] = None
    allModifications: Optional[bool] = False


class APIQueryParams(BaseModel):
    companiesParameters: Optional[CompaniesParameters]
    periodParameters: Optional[PeriodParameters]
    pageParameters: Optional[object]
