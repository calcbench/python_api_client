from dataclasses import dataclass
from typing import Sequence

from calcbench.api_client import _json_POST
from calcbench.api_query_params import (
    APIQueryParams,
    CompanyIdentifiers,
    Period,
    PeriodParameters,
    PeriodType,
)


@dataclass
class PressReleaseDataPoint:
    """
    Corresponds to PressReleaseDataPoint on the server
    """

    fact_id: int


@dataclass
class PressReleaseData:
    """
    Corresponds to PressReleaseDataWrapper on the server
    """

    facts: Sequence[PressReleaseDataPoint]


def press_release_raw(
    company_identifiers: CompanyIdentifiers,
    all_history: bool = False,
    year: int = None,
    period: Period = None,
    periodType: PeriodType = None,
) -> Sequence[PressReleaseData]:

    periodParameters: PeriodParameters = {
        "year": year,
        "period": period,
        "periodType": periodType,
    }
    payload: APIQueryParams = {
        "companiesParameters": {"companyIdentifiers": company_identifiers},
        "periodParameters": periodParameters,
        "pageParameters": {
            "standardizeBOPPeriods": True,
            "allowTwoToOneTableMatching": False,
            "matchToPreviousPeriod": False,
        },
    }
    d = _json_POST("pressReleaseGroups", payload)
    return d