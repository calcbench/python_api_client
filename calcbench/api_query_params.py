from datetime import datetime, date
from enum import Enum, IntEnum
from typing import Generic, Optional, Sequence, TypeVar, Union

from pydantic import BaseModel

from calcbench.models.period import PeriodArgument
from calcbench.models.period_type import PeriodType


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
    asOriginallyReported: Optional[bool] = False
    """
    Return only the first reported value
    """
    mostRecentOnly: Optional[bool] = False
    """
    Return only the most recent reported value.
    """


PageParametersT = TypeVar("PageParametersT")


class APIQueryParams(BaseModel, Generic[PageParametersT]):
    """
    Most Calcbench API endpoints take objects that look like this.
    """

    companiesParameters: Optional[CompaniesParameters]
    periodParameters: Optional[PeriodParameters]
    pageParameters: PageParametersT
