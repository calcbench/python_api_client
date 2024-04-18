from enum import Enum
from typing import Optional, Sequence

from calcbench.models.dimensional import DimensionalDataPoint

try:
    import pandas as pd
except ImportError:
    pass

from calcbench.api_client import (
    _json_POST,
)
from calcbench.api_query_params import CompanyIdentifiers, PeriodArgument, PeriodType


def dimensional(
    company_identifiers: CompanyIdentifiers = [],
    metrics: Sequence[str] = [],
    start_year: Optional[int] = None,
    start_period: PeriodArgument = None,
    end_year: Optional[int] = None,
    end_period: PeriodArgument = None,
    period_type: PeriodType = PeriodType.Annual,
    all_history: bool = True,
    trace_url: bool = False,
    as_originally_reported: bool = False,
) -> "pd.DataFrame":
    """
    Segments and Breakouts in a DataFrame

    The data behind the breakouts/segment page, https://www.calcbench.com/breakout.

    If there are no results an empty dataframe is returned

    :param sequence company_identifiers: Tickers/CIK codes. eg. ['msft', 'goog', 'appl', '0000066740']
    :param metrics: Specific line item to get, for instance `OperatingSegmentRevenue` or `ConcentrationRiskPercentageCustomer`, get the list @ https://www.calcbench.com/api/availableBreakouts, pass in the "databaseName"
    :param int start_year: first year of data to get
    :param start_period: first period of data to get.  0 for annual data, 1, 2, 3, 4 for quarterly data.
    :param int end_year: last year of data to get
    :param end_period: last period of data to get. 0 for annual data, 1, 2, 3, 4 for quarterly data.
    :param period_type: only applicable when other period data not supplied.
    :param trace_url: include a column with URL that point to the source document.
    :param as_originally_reported: Show the first reported, rather than revised, values
    :return: A list of points.  The points correspond to the lines @ https://www.calcbench.com/breakout.  For each requested metric there will be a the formatted value and the unformatted value denote bya  _effvalue suffix.  The label is the dimension label associated with the values.
    :rtype: pd.DataFrame

    Usage::

      >>> cb.dimensional(
      >>>   company_identifiers=cb.tickers(index="DJIA"),
      >>>   metrics=["OperatingSegmentRevenue", "OperatingSegmentOperatingIncome"],
      >>>   period_type="annual",
      >>> )

    """
    raw_data = dimensional_raw(
        company_identifiers=company_identifiers,
        metrics=metrics,
        start_year=start_year,
        start_period=start_period,
        end_year=end_year,
        end_period=end_period,
        period_type=period_type,
        all_history=all_history,
        as_originally_reported=as_originally_reported,
    )

    if not raw_data:
        return pd.DataFrame()

    df = pd.DataFrame(r.dict() for r in raw_data)

    df["fiscal_period"] = (
        df["fiscal_year"].astype(str) + "-" + df["fiscal_period"].astype(str)
    ).astype("string")

    df = df.set_index(
        ["ticker", "fiscal_period", "metric", "label", "standardized_label"]
    )

    columns = ["value", "CIK", "calendar_year", "calendar_period"]
    if trace_url:
        columns = columns + ["trace_url"]
    return df[columns].sort_index()


def dimensional_raw(
    company_identifiers: CompanyIdentifiers = [],
    metrics: Sequence[str] = [],
    start_year: Optional[int] = None,
    start_period: PeriodArgument = None,
    end_year: Optional[int] = None,
    end_period: PeriodArgument = None,
    period_type: PeriodType = PeriodType.Annual,
    all_history: bool = True,
    as_originally_reported: bool = False,
) -> Sequence[DimensionalDataPoint]:
    """Segments and Breakouts

    The data behind the breakouts/segment page, https://www.calcbench.com/breakout.

    :param company_identifiers: Tickers/CIK codes. eg. ['msft', 'goog', 'appl', '0000066740']
    :param metrics: Specific line item to get, for instance `OperatingSegmentRevenue` or `ConcentrationRiskPercentageCustomer`, get the list @ https://www.calcbench.com/api/availableBreakouts, pass in the "databaseName"
    :param start_year: first year of data to get
    :param start_period: first period of data to get.
    :param end_year: last year of data to get
    :param end_period: last period of data to get.
    :param period_type: Only applicable when other period data not supplied.
    :param all_history: Get data for all history
    :param as_originally_reported: Show the first reported, rather than revised, values

    Usage::
      >>> cb.dimensional_raw(company_identifiers=['fdx'],
      >>>   metrics=['OperatingSegmentRevenue'],
      >>>   start_year=2018
      >>> )

    """
    if len(metrics) == 0:
        raise (ValueError("Need to supply at least one metric."))

    payload = {
        "companiesParameters": {
            "entireUniverse": len(company_identifiers) == 0,
            "companyIdentifiers": company_identifiers,
        },
        "periodParameters": {
            "year": end_year or start_year,
            "period": start_period,
            "endYear": start_year,
            "endPeriod": end_period,
            "periodType": period_type,
            "asOriginallyReported": False,
            "allHistory": all_history,
        },
        "pageParameters": {
            "metrics": metrics,
            "dimensionName": "Segment",
            "AsOriginallyReported": as_originally_reported,
        },
    }
    return [DimensionalDataPoint(**p) for p in _json_POST("dimensionalData", payload)]
