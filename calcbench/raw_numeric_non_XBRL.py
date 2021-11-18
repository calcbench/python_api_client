import dataclasses
from dataclasses import dataclass
from datetime import datetime
from typing import Generator, Sequence

from calcbench.api_client import _try_parse_timestamp
from calcbench.api_query_params import CompanyIdentifiers, Period
from calcbench.raw_numeric_XBRL import (
    RAW_NON_XBRL_END_POINT,
    RawDataClause,
    _raw_data_raw,
)

try:
    import pandas as pd
except ImportError:
    "Can't find pandas, won't be able to use the functions that return DataFrames."
    pass


def non_XBRL_numeric_raw(
    company_identifiers: CompanyIdentifiers,
    entire_universe: bool = False,
    clauses: Sequence[RawDataClause] = [],
) -> Generator["NonXBRLFact", None, None]:
    """Non-XBRL numbers extracted from a variety of SEC filings, mainly earnings press-releases

    The data behind https://www.calcbench.com/nonXBRLRawData.

    A professional Calcbench subscription is required to access this data.

    :param company_identifiers: list of tickers or CIK codes
    :param entire_universe: Search all companies
    :param clauses: See the parameters that can be passed @ https://www.calcbench.com/api/rawDataNonXBRLPoints

    Usage:
        >>> clauses = [
        >>>     {
        >>>         "value": single_date.strftime("%Y-%m-%d"),
        >>>          "parameter": "filingDate",
        >>>         "operator": 1,
        >>>     },
        >>>     {"value": 2021, "parameter": "calendarYear", "operator": 1},
        >>>     {"value": "1Q", "parameter": "calendarPeriod", "operator": 1},
        >>> ]
        >>> d2 = list(cb.non_XBRL_numeric_raw(entire_universe=True, clauses=clauses))
    """
    for o in _raw_data_raw(
        company_identifiers=company_identifiers,
        entire_universe=entire_universe,
        clauses=clauses,
        end_point=RAW_NON_XBRL_END_POINT,
    ):
        yield NonXBRLFact(**o)


def non_XBRL_numeric(
    company_identifiers: CompanyIdentifiers = [],
    entire_universe: bool = False,
    clauses: Sequence[RawDataClause] = [],
) -> "pd.DataFrame":
    """Data frame of non-XBRL numbers.

    Data behind https://www.calcbench.com/pressReleaseRaw.
    A professional Calcbench subscription is required to access this data.

    :param company_identifiers: list of tickers or CIK codes
    :param entire_universe: Search all companies
    :param clauses: See the parameters that can be passed @ https://www.calcbench.com/api/rawDataNonXBRLPoints

    Usage:
        >>> clauses = [
        >>>    {"parameter": "fiscalYear", "operator": 1, "value": 2019},
        >>> ]
        >>> d = cb.non_XBRL_numeric(company_identifiers=['MSFT'], clauses=clauses)
    """

    facts = list(
        non_XBRL_numeric_raw(
            company_identifiers=company_identifiers,
            entire_universe=entire_universe,
            clauses=clauses,
        )
    )
    return pd.DataFrame(facts)


@dataclass
class NonXBRLFact:
    """
    Cooresponds to NonXBRLFact on the server
    """

    CIK: str
    UOM: str
    Value: float
    XBRLfilingID: int
    column_label: str
    companyID: int
    document: str
    entity_name: str
    extract_tag: str
    fact_id: int
    filingID: int
    filing_date: datetime
    filing_end_date: datetime
    filing_period: Period
    filing_year: int
    fiscal_period: Period
    fiscal_year: int
    is_guidance: bool
    is_non_gaap: bool
    label: str
    metric: str
    metric_id: int
    range_high: bool
    range_low: bool
    sec_filing_URL: str
    sec_html_url: str
    special_fact_type: str
    statement_type: int
    tabular_item: bool
    ticker: str

    def __init__(self, **kwargs):
        names = set([f.name for f in dataclasses.fields(self)])
        for k, v in kwargs.items():
            if k in names:
                if k in ("filing_end_date", "filing_date"):
                    v = _try_parse_timestamp(v)
                setattr(self, k, v)
