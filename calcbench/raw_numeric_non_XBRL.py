import dataclasses
from dataclasses import dataclass
from datetime import datetime
from enum import IntEnum
from typing import Generator, Sequence

from calcbench.api_client import _try_parse_timestamp
from calcbench.api_query_params import CompanyIdentifiers
from calcbench.models.period import Period
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


class StatementType(IntEnum):
    unset = -1
    docEntityInfo = 0
    IncomeStatement = 1
    BalanceSheet = 2
    CashFlow = 3
    Disclosure = 4
    StockholdersEquity = 5
    StatementOfComprehensiveIncome = 6
    IncomeStatementParenthetical = 11
    BalanceSheetParenthetical = 12
    CashFlowParenthetical = 13
    DisclosureParenthetical = 14
    stockholdersEquityParenthetical = 15
    StatementOfComprehensiveIncomeParenthetical = 16
    extensionAnchoring = 19


@dataclass
class NonXBRLFact:
    """
    Cooresponds to NonXBRLFact on the server
    """

    ticker: str
    CIK: str
    """
    Central Index Key
    """

    UOM: str
    """
    Examples: USD, PCT, PURE, GBP, EUR. This is specifed when a particular unit of measure appears with a number. 
    """

    Value: float
    XBRLfilingID: int
    column_label: str
    """
    full column label for that column
    """

    companyID: int
    """
    Calcbench entityID
    """

    document: str
    entity_name: str
    """
    Company name
    """

    extract_tag: str
    """
    Calcbench's attempt to create a unique tag/identifier (like an XBRL tag) for each concept in the filing. This is often useful for comparing data over time for a particular company, but would rarely be useful for comparing data across companies. 
    """

    fact_id: int
    """
    Calcbench database ID for that particular fact
    """

    filingID: int

    filing_date: datetime
    """
    Date the document was filed with the SEC
    """

    filing_end_date: datetime
    """
    The last day of the fiscal period to which this filing refers
    """

    filing_period: Period
    """
    Fiscal period to which the filing refers
    """

    filing_year: int
    """
    Fiscal year to which this filing refers
    """

    fiscal_period: Period
    """
    Fiscal period to which this fact refers
    """

    fiscal_year: int
    """
    Fiscal year to which this fact refers
    """

    is_guidance: bool
    """
    The value is guidance
    """

    is_non_gaap: bool
    """
    This value is a non-GAAP number
    """

    label: str
    """
    row label
    """

    metric: str
    metric_id: int
    range_high: bool
    """
    is high end of a stated range  
    """

    range_low: bool
    """
    is low end of a stated range (eg: 53% to 62%)
    """

    sec_filing_URL: str
    """
    The SEC url for this filing
    """

    special_fact_type: str
    statement_type: StatementType
    """
    Standardized statement type
    """

    tabular_item: bool

    def __init__(self, **kwargs):
        names = set([f.name for f in dataclasses.fields(self)])
        for k, v in kwargs.items():
            if k in names:
                if k in ("filing_end_date", "filing_date"):
                    v = _try_parse_timestamp(v)
                setattr(self, k, v)
