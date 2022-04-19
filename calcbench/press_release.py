from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Generator, Optional, Sequence
from decimal import Decimal

try:
    import pandas as pd
except ImportError:
    "Can't find pandas, won't be able to use the functions that return DataFrames."
    pass

from calcbench.api_client import _json_POST, set_field_values
from calcbench.api_query_params import (
    APIQueryParams,
    CompanyIdentifiers,
    Period,
    PeriodParameters,
)


class FormatType(str, Enum):
    Text = "text"


@dataclass
class PressReleaseDataPoint:
    """
    Corresponds to PressReleaseDataPoint on the server
    """

    fact_id: int
    sec_filing_id: int
    effective_value: Decimal
    reported_value: Decimal
    range_high_value: Decimal
    reported_value: Decimal
    is_instant_value: bool
    UOM: str
    format_type: FormatType
    period_start: datetime
    period_end: datetime
    presentation_order: int
    statement_type: str
    table_id: str

    def __init__(self, **kwargs) -> None:
        set_field_values(self, kwargs=kwargs)


@dataclass
class PressReleaseData:
    """
    Corresponds to PressReleaseDataWrapper on the server
    """

    facts: Sequence[PressReleaseDataPoint]
    entity_name: str
    entity_id: int
    ticker: str
    cik: str
    fiscal_year: int
    fiscal_period: int
    url: str
    date: datetime
    # filing_id: int
    filing_type: str
    fiscal_year_end_date: datetime
    date_reported: datetime

    def __init__(self, **kwargs) -> None:
        set_field_values(self, kwargs, {"date_reported"})


def press_release_raw(
    company_identifiers: CompanyIdentifiers,
    all_history: bool = False,
    year: Optional[int] = None,
    period: Optional[Period] = None,
) -> Generator[PressReleaseData, None, None]:

    periodParameters: PeriodParameters = {
        "year": year,
        "period": period,
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
    data = _json_POST("pressReleaseGroups", payload)
    if data:
        for d in data:
            yield PressReleaseData(**d)


CATEGORICAL_COLUMNS = ["fiscal_period", "UOM", "statement_type"]


def press_release_data(
    company_identifiers: CompanyIdentifiers,
    year: int,
    period: Period,
) -> "pd.DataFrame":
    filings = press_release_raw(
        company_identifiers=company_identifiers,
        year=year,
        period=period,
    )

    df = pd.DataFrame(
        [
            {
                **fact,
                **{
                    "ticker": filing.ticker,
                    "CIK": filing.cik,
                    "date_reported": filing.date_reported,
                },
            }
            for filing in filings
            for fact in filing.facts
        ]
    )
    if df.empty:
        return df
    for c in CATEGORICAL_COLUMNS:
        df[c] = pd.Categorical(df[c])
    return df
