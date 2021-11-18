from enum import IntEnum
from typing import TYPE_CHECKING, Optional, Sequence, Union

from calcbench.api_client import PeriodType

CentralIndexKey = Union[str, int]
Ticker = str
CalcbenchCompanyIdentifier = int
CompanyIdentifier = Union[Ticker, CentralIndexKey, CalcbenchCompanyIdentifier]
CompanyIdentifiers = Sequence[CompanyIdentifier]

if TYPE_CHECKING:
    # https://github.com/microsoft/pyright/issues/1358
    from typing import TypedDict
else:
    try:
        from typing import TypedDict
    except ImportError:
        from typing_extensions import TypedDict


class Period(IntEnum):
    Annual = 0
    Q1 = 1
    Q2 = 2
    Q3 = 3
    Q4 = 4


class CompaniesParameters(TypedDict):
    companyIdentifiers: CompanyIdentifiers


class PeriodParameters(TypedDict):
    year: int
    period: Period
    periodType: PeriodType


class APIQueryParams(TypedDict):
    companiesParameters: Optional[CompaniesParameters]
    periodParameters: Optional[PeriodParameters]
    pageParameters: Optional[object]
