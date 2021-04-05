import dataclasses
from dataclasses import dataclass
from datetime import datetime
from typing import Sequence

from calcbench.api_client import CompanyIdentifiers, Period, _try_parse_timestamp
from calcbench.raw_numeric_XBRL import (
    RAW_NON_XBRL_END_POINT,
    RawDataClause,
    raw_data_raw,
)

try:
    import pandas as pd
except ImportError:
    "Can't find pandas, won't be able to use the functions that return DataFrames."
    pass


def non_XBRL_numeric_raw(
    company_identifiers: CompanyIdentifiers = [],
    entire_universe: bool = False,
    clauses: Sequence[RawDataClause] = [],
) -> Sequence["NonXBRLFact"]:
    """Non-XBRL numbers extracted from a variety of SEC filings

    The data behind https://www.calcbench.com/nonXBRLRawData.

    A professional Calcbench subscription is required to access this data.

    :param company_identifiers: list of tickers or CIK codes
    :param entire_universe: Search all companies
    :param clauses: a sequence of dictionaries which the data is filtered by.  A clause is a dictionary with "value", "parameter" and "operator" keys.  See the parameters that can be passed @ https://www.calcbench.com/api/rawDataNonXBRLPoints

    """
    for o in raw_data_raw(
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

    Data behind https://www.calcbench.com/nonXBRLRawData
    A professional Calcbench subscription is required to access this data.

    :param company_identifiers: list of tickers or CIK codes
    :param entire_universe: Search all companies
    :param clauses: a sequence of dictionaries which the data is filtered by.  A clause is a dictionary with "value", "parameter" and "operator" keys.  See the parameters that can be passed @ https://www.calcbench.com/api/rawDataNonXBRLPoints

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
    Cooresponds to XBRLDisclosure on the server
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
