from enum import Enum, IntEnum
from typing import TYPE_CHECKING, Optional, Sequence, Union

try:
    from typing import Literal
except ImportError:
    from typing_extensions import Literal

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


PeriodArgument = Optional[Union[Period, Literal[0, 1, 2, 3, 4]]]


class CompaniesParameters(TypedDict):
    companyIdentifiers: CompanyIdentifiers


class PeriodParameters(TypedDict, total=False):
    year: Optional[int]
    period: Optional[Period]
    periodType: Optional[PeriodType]


class APIQueryParams(TypedDict):
    companiesParameters: Optional[CompaniesParameters]
    periodParameters: Optional[PeriodParameters]
    pageParameters: Optional[object]
