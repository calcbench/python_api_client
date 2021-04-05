from dataclasses import dataclass
import dataclasses
from enum import IntEnum
from typing import Literal, Mapping, Sequence, TYPE_CHECKING
from calcbench.api_client import (
    CompanyIdentifiers,
    Period,
    _json_POST,
    _try_parse_timestamp,
)

if TYPE_CHECKING:
    # https://github.com/microsoft/pyright/issues/1358
    from typing import TypedDict
else:
    try:
        from typing import TypedDict
    except ImportError:
        from typing_extensions import TypedDict

try:
    import pandas as pd
except ImportError:
    "Can't find pandas, won't be able to use the functions that return DataFrames."
    pass

RAW_XBRL_END_POINT = "rawXBRLData"
RAW_NON_XBRL_END_POINT = "rawNonXBRLData"
END_POINTS = Literal[RAW_XBRL_END_POINT, RAW_NON_XBRL_END_POINT]


class Operator(IntEnum):
    Equals = 1
    Contains = 10


class RawDataClause(TypedDict):
    value: str
    parameter: str
    operator: Operator


def raw_data(
    company_identifiers: CompanyIdentifiers = [],
    entire_universe=False,
    clauses: Sequence[RawDataClause] = [],
    end_point: END_POINTS = RAW_XBRL_END_POINT,
) -> "pd.DataFrame":
    """As-reported data.

    :param list(str) company_identifiers: list of tickers or CIK codes
    :param bool entire_universe: Search all companies
    :param list(dict) clauses: a sequence of dictionaries which the data is filtered by.  A clause is a dictionary with "value", "parameter" and "operator" keys.  See the parameters that can be passed @ https://www.calcbench.com/api/rawdataxbrlpoints
    :param str end_point: 'rawXBRLData' for facts tagged by XBRL, 'rawNONXBRLData' for facts parsed/extracted from non-XBRL tagged documents

    Usage:
        >>> clauses = [
        >>>     {"value": "Revenues", "parameter": "XBRLtag", "operator": 10},
        >>>     {"value": "Y", "parameter": "fiscalPeriod", "operator": 1},
        >>>     {"value": "2018", "parameter": "fiscalYear", "operator": 1}
        >>> ]
        >>> cb.raw_xbrl(company_identifiers=['mmm'], clauses=clauses)
    """
    if end_point not in (RAW_XBRL_END_POINT, RAW_NON_XBRL_END_POINT):
        raise ValueError(
            f"end_point must be either {RAW_XBRL_END_POINT} or {RAW_NON_XBRL_END_POINT}"
        )
    d = raw_xbrl_raw(
        company_identifiers=company_identifiers,
        entire_universe=entire_universe,
        clauses=clauses,
        end_point=end_point,
    )
    df = pd.DataFrame(d)
    for date_column in [
        "filing_date",
        "filing_end_date",
        "period_end",
        "period_start",
        "period_instant",
    ]:
        df[date_column] = pd.to_datetime(df[date_column])  # type: ignore
    df.rename({"Value": "value"}, inplace=True)  # type: ignore
    return df


raw_xbrl = raw_data


def raw_data_raw(
    company_identifiers: CompanyIdentifiers = [],
    entire_universe=False,
    clauses: Sequence[RawDataClause] = [],
    end_point: END_POINTS = RAW_XBRL_END_POINT,
) -> Sequence[Mapping[str, object]]:
    """Data as reported in the XBRL documents

    :param list(str) company_identifiers: list of tickers or CIK codes
    :param bool entire_universe: Search all companies
    :param list(dict) clauses: a sequence of dictionaries which the data is filtered by.  A clause is a dictionary with "value", "parameter" and "operator" keys.  See the parameters that can be passed @ https://www.calcbench.com/api/rawdataxbrlpoints

    Usage:
        >>> clauses = [
        >>>     {"value": "Revenues", "parameter": "XBRLtag", "operator": 10},
        >>>     {"value": "Y", "parameter": "fiscalPeriod", "operator": 1},
        >>>     {"value": "2018", "parameter": "fiscalYear", "operator": 1}
        >>> ]
        >>> cb.raw_xbrl_raw(company_identifiers=['mmm'], clauses=clauses)
    """
    payload = {
        "companiesParameters": {
            "companyIdentifiers": company_identifiers,
            "entireUniverse": entire_universe,
        },
        "pageParameters": {"clauses": clauses},
    }
    results = _json_POST(end_point, payload)
    if end_point == RAW_XBRL_END_POINT:
        for result in results:
            if result["dimension_string"]:
                result["dimensions"] = {
                    d.split(":")[0]: d.split(":")[1]
                    for d in result["dimension_string"].split(",")
                }
            else:
                result["dimensions"] = []

    return results


raw_xbrl_raw = raw_data_raw


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
    filing_date: str
    filing_end_date: str
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


def non_XBRL_numeric_raw(
    company_identifiers: CompanyIdentifiers = [],
    entire_universe=False,
    clauses: Sequence[RawDataClause] = [],
) -> Sequence[NonXBRLFact]:
    """Non-XBRL numbers extracted from a variety of SEC filings
    The data behind https://www.calcbench.com/nonXBRLRawData.

    A professional Calcbench subscription is required to access this data.
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
    entire_universe=False,
    clauses: Sequence[RawDataClause] = [],
) -> "pd.DataFrame":
    """Data frame of non-XBRL numbers.
    Data behind https://www.calcbench.com/nonXBRLRawData
    A professional Calcbench subscription is required to access this data.
    """
    facts = list(
        non_XBRL_numeric_raw(
            company_identifiers=company_identifiers,
            entire_universe=entire_universe,
            clauses=clauses,
        )
    )
    return pd.DataFrame(facts)
