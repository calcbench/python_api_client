from typing import TYPE_CHECKING, Optional, Sequence, Union

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


class CompaniesParameters(TypedDict):
    companyIdentifiers: CompanyIdentifiers


class APIQueryParams(TypedDict):
    companiesParameters: Optional[CompaniesParameters]
    periodParameters: Optional[object]
    pageParameters: Optional[object]
