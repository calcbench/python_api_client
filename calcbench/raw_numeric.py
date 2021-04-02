from enum import IntEnum
from typing import Literal, Sequence, TYPE_CHECKING
from calcbench.api_client import CompanyIdentifiers, _json_POST

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
):
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


def non_XBRL_numeric_raw():
    """Non-XBRL numbers extracted from a variety of SEC filings
    The data behind https://www.calcbench.com/nonXBRLRawData.

    A professional Calcbench subscription is required to access this data.
    """
    pass