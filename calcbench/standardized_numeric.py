import warnings
from datetime import date, datetime
from typing import TYPE_CHECKING, Any, Optional, Sequence, Union


from calcbench.api_query_params import (
    APIQueryParams,
    CompaniesParameters,
    CompanyIdentifiers,
    CompanyIdentifierScheme,
    Period,
    PeriodArgument,
    PeriodParameters,
    PeriodType,
)

if TYPE_CHECKING:
    # https://github.com/microsoft/pyright/issues/1358
    from typing import TypedDict
else:
    try:
        from typing import TypedDict
    except ImportError:
        from typing_extensions import TypedDict

from calcbench.api_client import _json_POST, logger

try:
    import numpy as np
    import pandas as pd

    period_number = pd.api.types.CategoricalDtype(  # type: ignore
        categories=[1, 2, 3, 4, 5, 6, 0], ordered=True
    )  # So annual is last in sorting.  5 and 6 are first half and 3QCUM.
except ImportError:
    "Can't find pandas, won't be able to use the functions that return DataFrames."
    pass


class TraceData(TypedDict):
    local_name: str
    negative_weight: str
    XBRL_fact_value: Union[str, float, int]
    fact_id: int
    dimensions: int
    trace_url: str


class StandardizedPoint(TypedDict):
    """
    Replicates MappedDataPoint on the server
    """

    metric: str
    value: Union[str, float, int]
    calendar_year: int
    calendar_period: Period
    fiscal_year: int
    fiscal_period: Period
    trace_facts: Sequence[TraceData]
    ticker: str
    calcbench_entity_id: int
    filing_type: str  # 10-K, 10-Q, 8-K, PRESSRELEASE, etc.
    preliminary: bool
    CIK: str
    trace_url: Optional[str]
    period: Any  # pandas period
    date_reported: str  # only on PIT points


def standardized_raw(
    company_identifiers: CompanyIdentifiers = [],
    metrics: Sequence[str] = [],
    start_year: Optional[int] = None,
    start_period: PeriodArgument = None,
    end_year: Optional[int] = None,
    end_period: PeriodArgument = None,
    entire_universe=False,
    accession_id: Optional[int] = None,
    point_in_time: bool = False,
    include_trace: bool = False,
    update_date: Optional[date] = None,
    all_history=False,
    year: Optional[int] = None,
    period: PeriodArgument = None,
    period_type: Optional[PeriodType] = None,
    include_preliminary: bool = False,
    use_fiscal_period: bool = False,
    all_face: bool = False,
    all_footnotes: bool = False,
    include_xbrl: bool = False,
    filing_id: Optional[int] = None,
    all_non_GAAP: bool = False,
    all_metrics=False,
) -> Sequence[StandardizedPoint]:
    """Standardized data.

    Get normalized data from Calcbench.  Each point is normalized by economic concept and time period.

    :param company_identifiers: a sequence of tickers (or CIK codes), eg ['msft', 'goog', 'appl']
    :param metrics: a sequence of metrics, see the full list @ https://www.calcbench.com/home/standardizedmetrics eg. ['revenue', 'accountsreceivable']
    :param start_year: first year of data
    :param start_period: first quarter to get, for annual data pass 0, for quarters pass 1, 2, 3, 4
    :param end_year: last year of data
    :param end_period: last_quarter to get, for annual data pass 0, for quarters pass 1, 2, 3, 4
    :param entire_universe: Get data for all companies, this can take a while, talk to Calcbench before you do this in production.
    :param accession_id: Calcbench Accession ID
    :param include_trace: Include the facts used to calculate the normalized value.
    :param year: Get data for a single year, defaults to annual data.
    :param period_type: Either "annual" or "quarterly"
    :param include_preliminary: Include data from non-XBRL 8-Ks and press releases.
    :param exclude_errors: Run another level of error detections, only works for PIT preliminary
    :param filing_id: Filing id for which to get data.  corresponds to the filing_id in the objects returned by the filings API.
    :param all_non_GAAP: include all non-GAAP metrics from earnings press releases such as EBITDA_NonGAAP.  This is implied when querying by `filing_id`.
    :param all_metrics: All metrics.
    :return: A list of dictionaries with keys ['ticker', 'calendar_year', 'calendar_period', 'metric', 'value'].

    """
    if [
        bool(company_identifiers),
        bool(entire_universe),
        bool(accession_id),
        bool(filing_id),
    ].count(True) != 1:
        raise ValueError(
            "Must pass either company_identifiers, accession id, filing_id, or entire_universe=True"
        )
    if not any(
        [
            start_year,
            end_year,
            update_date,
            year,
            accession_id,
            filing_id,
            all_history,
            period_type,
        ]
    ):
        raise ValueError("Need to specify a period qualifier")

    if (accession_id or filing_id) and any(
        [
            company_identifiers,
            start_year,
            start_period,
            end_year,
            end_period,
            entire_universe,
            year,
            period,
        ]
    ):
        raise ValueError(
            "Accession/Filing IDs are specific to a filing, no other qualifiers make sense."
        )

    if period is not None:
        if start_period or end_period:
            raise ValueError(
                "Use period for a single period.  start_period and end_period are for ranges."
            )
        if period not in ("Y", 0) and (start_year or end_year):
            raise ValueError("With start_year or end_year only annual period works")
        start_period = end_period = period

    if year:
        if start_year or end_year:
            raise ValueError(
                "Use year for a single period.  start_year and end_year for ranges."
            )
        start_year = end_year = year

    if period_type and period_type.lower() not in (
        "annual",
        "quarterly",
        "combined",
        "ttm",
    ):
        raise ValueError(
            'period_type must be in "annual", "quarterly", "combined", "ttm"'
        )

    if include_preliminary and not point_in_time:
        raise ValueError("include_preliminary only works for PIT")

    if metrics and (all_face or all_footnotes):
        raise ValueError(
            "specifying metrics with all_face or all_footnotes does not make sense"
        )

    if not any(
        [all_face, all_footnotes, metrics, filing_id, all_non_GAAP, all_metrics]
    ):
        raise ValueError(
            "need to specify a metrics argument, 'all_face', 'all_foonotes', 'all_non_GAAP', 'all_metrics' or a metric"
        )

    if isinstance(metrics, str):
        raise TypeError("metrics should be a list of strings")

    if isinstance(company_identifiers, str):
        raise TypeError("company_identifiers should be a list of strings")

    if all_history and any(
        [year, period, start_period, start_year, end_period, end_year]
    ):
        raise ValueError("all_history with other period arguments does not make sense")

    if (
        period_type == PeriodType.TrailingTwelveMonths
        and all_history
        and len(metrics) != 1
    ):
        raise ValueError("TTM only works with one metric for all history")
    try:
        start_year = int(start_year)  # type: ignore
    except (ValueError, TypeError):
        pass
    try:
        start_period = int(start_period)  # type: ignore
    except (ValueError, TypeError):
        pass
    try:
        end_year = int(end_year)  # type: ignore
    except (ValueError, TypeError):
        pass
    try:
        end_period = int(end_period)  # type: ignore
    except (ValueError, TypeError):
        pass

    period_parameters: PeriodParameters = {
        "year": start_year,
        "period": start_period,
        "endYear": end_year,
        "endPeriod": end_period,
        "allHistory": all_history,
        "updateDate": update_date and update_date.isoformat(),
        "periodType": period_type,
        "useFiscalPeriod": use_fiscal_period,
        "accessionID": accession_id,
        "filingID": filing_id,
    }  # type: ignore

    companies_parameters: CompaniesParameters = {
        "entireUniverse": entire_universe,
        "companyIdentifiers": list(company_identifiers),
    }

    payload: APIQueryParams = {
        "pageParameters": {
            "metrics": metrics,
            "includeTrace": include_trace,
            "pointInTime": point_in_time,
            "includePreliminary": include_preliminary,
            "allFootnotes": all_footnotes,
            "allface": all_face,
            "includeXBRL": include_xbrl,
            "allNonGAAP": all_non_GAAP,
            "allMetrics": all_metrics,
        },
        "periodParameters": period_parameters,
        "companiesParameters": companies_parameters,
    }

    return _json_POST("mappedData", payload)


ORDERED_COLUMNS = [
    "ticker",
    "metric",
    "fiscal_year",
    "fiscal_period",
    "value",
    "revision_number",
    "preliminary",
    "XBRL",
    "date_reported",
    "filing_type",
    "CIK",
    "calcbench_entity_id",
    "period_start",
    "period_end",
    "calendar_year",
    "calendar_period",
    "filing_accession_number",
    "trace_url",
]


def _build_data_frame(raw_data: Sequence[StandardizedPoint]) -> "pd.DataFrame":
    """
    The order of the columns should remain constant
    """
    if not raw_data:
        return pd.DataFrame()
    data = pd.DataFrame(raw_data)
    new_columns = list(set(data.columns) - set(ORDERED_COLUMNS))
    data = data.reindex(columns=ORDERED_COLUMNS + new_columns)
    data = data.drop(columns=["trace_facts"], errors="ignore")  # type: ignore
    return data


def point_in_time(
    company_identifiers: CompanyIdentifiers = [],
    all_footnotes: bool = False,
    metrics: Sequence[str] = [],
    all_history: bool = False,
    entire_universe: bool = False,
    start_year: Optional[int] = None,
    start_period: PeriodArgument = None,
    end_year: Optional[int] = None,
    end_period: PeriodArgument = None,
    period_type: Optional[PeriodType] = None,
    use_fiscal_period: bool = False,
    include_preliminary: bool = False,
    all_face: bool = False,
    include_xbrl: bool = True,
    accession_id: Optional[int] = None,
    include_trace: bool = False,
    set_index: bool = False,
    _point_in_time_mode=True,
    filing_id: Optional[int] = None,
    all_non_GAAP: bool = False,
) -> "pd.DataFrame":
    """Point-in-Time Data

    .. deprecated:: 6.0.1
        Use :func:`standardized` with the `point_in_time=True` argument.

    Standardized data with a timestamp when it was published by Calcbench.


    A record is returned for each filing in which a metric value changed (was revised).

    If the company files an 8-K with revenue = $100 then a week later files a 10-K with revenue = $100 one record will be returned for that period.  It will have preliminary=True and XBRL=True.

    If the company files an 8-K with revenue = $100 and then a week later files a 10-K with revenue = $200, two records will be returned.  One with revenue = $100, a revision_number=0, date_reported of 8-K, preliminary=True and XBRL=False.  You will see a second line with revenue = $200, a revision_number=1, and date_reported of the 10-K, preliminary=False and XBRL=False.

    If the value is revised in subsequent XBRL filings you will see a record for each filing with an incremented revision number.

    :param accession_id: Unique identifier for the filing for which to recieve data.  Pass this to recieve data for one filing.  Same as calcbench_id in filings objects
    :param all_face: Retrieve all of the points from the face financials, income/balance/statement of cash flows
    :param all_footnotes: Retrive all of the points from the footnotes to the financials
    :param include_preliminary: Include facts from non-XBRL earnings press-releases and 8-Ks.
    :param include_xbrl: Include facts from XBRL 10-K/Qs.
    :param include_trace: Include a URL that points to the source document.
    :param set_index: Set a useful index on the returned DataFrame
    :param _point_in_time_mode: DO NOT USE.  For debugging only.
    :param filing_id: Filing id for which to get data.  corresponds to the filing_id in the objects returned by the filings API.
    :param all_non_GAAP: include all non-GAAP metrics from earnings press releases such as EBITDA_NonGAAP.  This is implied when querying by `filing_id`.
    :return: DataFrame of facts

    Columns:


    value
       The value of the fact
    revision_number
       0 indicates an original, unrevised value for this fact. 1, 2, 3... indicates subsequent revisions to the fact value.  https://knowledge.calcbench.com/hc/en-us/search?utf8=%E2%9C%93&query=revisions&commit=Search
    preliminary
        True indicates the number was parsed from non-XBRL 8-K or press release from the wire
    XBRL
        Indicates the number was parsed from XBRL
    period_start
       First day of the fiscal period for this fact
    period_end
       Last day of the fiscal period for this fact
    date_reported
       Timestamp (EST) when Calcbench published this fact.

       In some cases, particularly prior to 2015, this will be the filing date of the document as recorded by the SEC.  To exclude these points remove points where the hour is 0.
    metric
       The metric name, see the definitions @ https://www.calcbench.com/home/standardizedmetrics
    calendar_year
       The calendar year for this fact.  https://knowledge.calcbench.com/hc/en-us/articles/223267767-What-are-Calendar-Years-and-Periods-What-is-TTM-
    calendar_period
       The calendar period for this fact
    fiscal_year
       Company reported fiscal year for this fact
    fiscal_period
       Company reported fiscal period for this fact
    ticker
       Ticker of reporting company
    CIK
       SEC assigned Central Index Key for reporting company
    calcbench_entity_id
       Internal Calcbench identifier for reporting company
    filing_type
       The document type this fact came from, 10-K|Q, S-1 etc...



    Usage::
       >>> calcbench.point_in_time(company_identifiers=["msft", "goog"],
       >>>                          all_history=True,
       >>>                          all_face=True,
       >>>                          all_footnotes=True)

    .. _Example: https://github.com/calcbench/notebooks/blob/master/standardized_numeric_point_in_time.ipynb


    """

    warnings.warn("Prefer standardized with point_in_time=True.", DeprecationWarning)
    data = standardized_raw(
        company_identifiers=company_identifiers,
        all_face=all_face,
        all_footnotes=all_footnotes,
        point_in_time=_point_in_time_mode,
        metrics=metrics,
        all_history=all_history,
        entire_universe=entire_universe,
        start_year=start_year,
        start_period=start_period,
        end_year=end_year,
        end_period=end_period,
        period_type=period_type,
        use_fiscal_period=use_fiscal_period,
        include_preliminary=include_preliminary,
        accession_id=accession_id,
        include_trace=include_trace,
        include_xbrl=include_xbrl,
        filing_id=filing_id,
        all_non_GAAP=all_non_GAAP,
    )

    if not data:
        return pd.DataFrame()
    data = _build_data_frame(data)

    sort_columns = ["ticker", "metric"]

    if "calendar_period" in data.columns:
        data.calendar_period = data.calendar_period.astype(period_number)  # type: ignore
        sort_columns.extend(["calendar_year", "calendar_period"])
    if "fiscal_period" in data.columns:
        data.fiscal_period = data.fiscal_period.astype(period_number)  # type: ignore
    if "calendar_period" in data.columns:
        data.calendar_period = data.calendar_period.astype(period_number)  # type: ignore
    if not data.empty:
        for date_column in ["date_reported", "period_end", "period_start"]:
            if date_column in data.columns:
                data[date_column] = pd.to_datetime(data[date_column], errors="coerce")  # type: ignore
    if set_index:
        if not period_type:
            data["period"] = (
                data.fiscal_year.astype(str) + "-" + data.fiscal_period.astype(str)
            )
        else:
            if period_type == PeriodType.Quarterly:
                data = data[
                    data.fiscal_period != 0
                ]  # Sometime annual data gets into the quarterly stream
                data["period"] = pd.PeriodIndex(
                    year=data.fiscal_year, quarter=data.fiscal_period, freq="Q"
                )
            elif period_type == PeriodType.Annual:
                data["period"] = pd.PeriodIndex(data["fiscal_year"], freq="A")
        data.drop(["fiscal_year", "fiscal_period"], axis=1, inplace=True)
        data = data.set_index(
            ["ticker", "metric", "period", "date_reported"]
        ).sort_index()
    else:
        data = data.sort_values(sort_columns).reset_index(drop=True)
    return data


def standardized_data(
    company_identifiers: CompanyIdentifiers = [],
    metrics: Sequence[str] = [],
    start_year: Optional[int] = None,
    start_period: PeriodArgument = None,
    end_year: Optional[int] = None,
    end_period: PeriodArgument = None,
    entire_universe: bool = False,
    point_in_time: bool = False,
    year: Optional[int] = None,
    period: PeriodArgument = None,
    all_history: bool = False,
    period_type: Optional[PeriodType] = None,
    trace_hyperlinks: bool = False,
    use_fiscal_period: bool = False,
    company_identifier_scheme: CompanyIdentifierScheme = CompanyIdentifierScheme.Ticker,
    accession_id: Optional[int] = None,
) -> "pd.DataFrame":
    """Standardized Data.

    Metrics are standardized by economic concept and time period.

    The data behind the multi-company page, https://www.calcbench.com/multi.

    Example https://github.com/calcbench/notebooks/blob/master/python_client_api_demo.ipynb

    :param company_identifiers: Tickers/CIK codes. eg. ['msft', 'goog', 'appl', '0000066740']
    :param metrics: Standardized metrics.  Full list @ https://www.calcbench.com/home/standardizedmetrics eg. ['revenue', 'accountsreceivable']
    :param start_year: first year of data
    :param start_period: first quarter to get, for annual data pass 0, for quarters pass 1, 2, 3, 4
    :param end_year: last year of data
    :param end_period: last_quarter to get, for annual data pass 0, for quarters pass 1, 2, 3, 4
    :param entire_universe: Get data for all companies, this can take a while, talk to Calcbench before you do this in production.
    :param accession_id: Calcbench Accession ID
    :param year: Get data for a single year, defaults to annual data.
    :param period_type: Either "annual" or "quarterly".
    :param trace_hyperlinks: Values are URLs to the source documents
    :return: Dataframe with the periods as the index and columns indexed by metric and ticker

    Usage::

      >>> d = calcbench.standardized_data(company_identifiers=['msft', 'goog'],
      >>>                                 metrics=['revenue', 'assets'],
      >>>                                 all_history=True,
      >>>                                 period_type='annual')

      >>> # Make it look like Compustat data
      >>> d.stack(level=1)

    """

    if all_history and not period_type:
        raise ValueError("For all history you must specify a period_type")
    if period_type == PeriodType.Combined:
        raise ValueError(
            "Cannot use combined because we can't build the time-series index"
        )

    data = standardized_raw(
        company_identifiers=list(company_identifiers),
        metrics=metrics,
        start_year=start_year,
        start_period=start_period,
        end_year=end_year,
        end_period=end_period,
        entire_universe=entire_universe,
        point_in_time=point_in_time,
        year=year,
        period=period,
        all_history=all_history,
        period_type=period_type,
        use_fiscal_period=use_fiscal_period,
        accession_id=accession_id,
        include_trace=trace_hyperlinks,
    )
    if not data:
        warnings.warn("No data found")
        return pd.DataFrame()

    quarterly = (start_period and end_period) or period_type in (
        PeriodType.Quarterly,
        PeriodType.TrailingTwelveMonths,
    )
    if quarterly:
        build_period = _build_quarter_period
    else:
        build_period = _build_annual_period

    metrics_found = set()
    for d in data:
        d["period"] = build_period(d, use_fiscal_period)
        d["ticker"] = d["ticker"].upper()
        try:  # This is probably not necessary, we're doing it in the dataframe. akittredge January 2017.
            value = float(d["value"])
            if trace_hyperlinks:
                value = f'=HYPERLINK("{d["trace_url"]}", {value})'
            d["value"] = value
        except (ValueError, KeyError):
            pass
        metrics_found.add(d["metric"])

    missing_metrics = set(metrics) - metrics_found
    if missing_metrics:
        warnings.warn("Did not find metrics {0}".format(missing_metrics))
    data = pd.DataFrame(data)
    for column in ["metric", "ticker", "CIK"]:
        data[column] = pd.Categorical(data[column])  # type: ignore

    data.set_index(
        keys=[f"{company_identifier_scheme}", "metric", "period"], inplace=True  # type: ignore
    )  # type: ignore

    try:
        data = data.unstack("metric")  # type: ignore
    except ValueError as e:
        if str(e) == "Index contains duplicate entries, cannot reshape":
            duplicates = data[data.index.duplicated()]["value"]  # type: ignore
            logger.error("Duplicate values \n {0}".format(duplicates))
        raise
    data = data["value"]

    for column_name in data.columns.values:
        # Try to make the columns the right type
        try:
            data[column_name] = pd.to_numeric(data[column_name], errors="raise")
        except ValueError:
            if "date" in column_name.lower():
                data[column_name] = pd.to_datetime(data[column_name], errors="coerce")  # type: ignore

    for missing_metric in missing_metrics:
        data[missing_metric] = np.nan  # We want columns for every requested metric.
    data = data.unstack(f"{company_identifier_scheme}")
    return data


def standardized(
    company_identifiers: CompanyIdentifiers = [],
    metrics: Sequence[str] = [],
    fiscal_year: Optional[int] = None,
    fiscal_period: PeriodArgument = None,
    point_in_time: bool = False,
    filing_id: Optional[int] = None,
):
    """Standardized Numeric Data.



    The data behind the multi-company page, https://www.calcbench.com/multi.

    Example https://github.com/calcbench/notebooks/blob/master/python_client_api_demo.ipynb

    :param company_identifiers: Tickers/CIK codes. eg. ['msft', 'goog', 'appl', '0000066740'].  If not specified get data for all companies.
    :param metrics: Standardized metrics.  Full list @ https://www.calcbench.com/home/standardizedmetrics eg. ['revenue', 'accountsreceivable'].  If not specified get all metrics.
    :param fiscal_year: Fiscal year for which to get data.  If not specified get all history.
    :param fiscal_period: Fiscal period for which to get data.  If not specified get all history.
    :param point_in_time: Include timestamps when data was published and revision chains.
    :param filing_id: Filing ID for which to get data.  Get all of the data reported in this filing.
    :return: Dataframe

    Usage::

      >>> d = calcbench.standardized(company_identifiers=['msft'],
      >>>                                 point_in_time=True,)
      >>> )
      >>> # Put the data in a format amiable to arithmetic on columns
      >>> d = calcbench.standardized(company_identifiers=['msft', 'orcl'], metrics=['StockholdersEquity', 'NetIncome'])
      >>> d = d.unstack("metric")["value"]
      >>> return_on_equity = d['NetIncome'] / d['StockholdersEquity']
    """
    data = standardized_raw(
        company_identifiers=company_identifiers,
        all_history=not fiscal_year,
        year=fiscal_year,
        period=fiscal_period,
        point_in_time=point_in_time,
        metrics=metrics,
        entire_universe=not (company_identifiers or filing_id),
        filing_id=filing_id,
        all_metrics=not metrics,
        use_fiscal_period=True,
        include_preliminary=point_in_time,
    )

    data = _build_data_frame(data)
    if data.empty:
        return data
    data["fiscal_period"] = (
        data["fiscal_year"].astype(str) + "-" + data["fiscal_period"].astype(str)
    )
    data = data.drop("fiscal_year", axis=1)

    if "calendar_period" in data.columns:
        data.calendar_period = data.calendar_period.astype(period_number)  # type: ignore

    if "calendar_period" in data.columns:
        data.calendar_period = data.calendar_period.astype(period_number)  # type: ignore

    for date_column in ["date_reported", "period_end", "period_start"]:
        if date_column in data.columns:
            data[date_column] = pd.to_datetime(data[date_column], errors="coerce")  # type: ignore
    index_columns = [
        "ticker",
        "metric",
        "fiscal_period",
    ]
    if point_in_time:
        index_columns = index_columns + ["date_reported"]
        data["date_downloaded"] = datetime.now()
    data = data.set_index(index_columns)
    return data


def _build_quarter_period(
    data_point: StandardizedPoint, use_fiscal_period: bool
) -> "pd.Period":
    try:
        return pd.Period(  # type: ignore
            year=data_point.pop(
                "fiscal_year" if use_fiscal_period else "calendar_year"
            ),
            quarter=data_point.pop(
                "fiscal_period" if use_fiscal_period else "calendar_period"
            ),
            freq="q",
        )
    except ValueError:
        # DEI points (entity_name) etc, don't have periods.
        return pd.Period()  # type: ignore


def _build_annual_period(
    data_point: StandardizedPoint, use_fiscal_period: bool
) -> "pd.Period":
    data_point.pop("fiscal_period" if use_fiscal_period else "calendar_period")
    return pd.Period(  # type: ignore
        year=data_point.pop("fiscal_year" if use_fiscal_period else "calendar_year"),
        freq="a",
    )


normalized_data = standardized_data  # used to call it normalized_data.
normalized_dataframe = standardized_data
normalized_raw = standardized_raw
