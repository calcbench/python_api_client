from datetime import date, datetime
from typing import Optional, Sequence, Union


from calcbench.api_query_params import (
    APIQueryParams,
    CompaniesParameters,
    CompanyIdentifiers,
    DateRange,
    PeriodArgument,
    PeriodParameters,
    PeriodType,
)
from calcbench.models.standardized import StandardizedPoint
from calcbench.standardized_parameters import StandardizedParameters


from calcbench.api_client import _json_POST

try:
    import pandas as pd

    period_number = pd.api.types.CategoricalDtype(  # type: ignore
        categories=[1, 2, 3, 4, 5, 6, 0, -1], ordered=True
    )  # So annual is last in sorting.  5 and 6 are first half and 3QCUM.
except ImportError:
    "Can't find pandas, won't be able to use the functions that return DataFrames."
    pass


def standardized_raw(
    company_identifiers: CompanyIdentifiers = [],
    metrics: Sequence[str] = [],
    start_year: Optional[int] = None,
    start_period: PeriodArgument = None,
    end_year: Optional[int] = None,
    end_period: PeriodArgument = None,
    entire_universe: bool = False,
    point_in_time: bool = False,
    include_trace: bool = False,
    all_history: bool = False,
    year: Optional[int] = None,
    period: PeriodArgument = None,
    period_type: Optional[PeriodType] = None,
    use_fiscal_period: bool = False,
    all_face: bool = False,
    all_footnotes: bool = False,
    filing_id: Optional[int] = None,
    all_non_GAAP: bool = False,
    all_metrics: bool = False,
    pit_V2: Optional[bool] = False,
    start_date: Optional[Union[datetime, date]] = None,
    end_date: Optional[Union[datetime, date]] = None,
    XBRL_only: Optional[bool] = False,
    all_modifications: Optional[bool] = False,
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
    :param include_trace: Include the facts used to calculate the normalized value.
    :param year: Get data for a single year, defaults to annual data.
    :param period_type: Either "annual" or "quarterly"
    :param filing_id: Filing id for which to get data.  corresponds to the filing_id in the objects returned by the filings API.
    :param all_non_GAAP: include all non-GAAP metrics from earnings press releases such as EBITDA_NonGAAP.  This is implied when querying by `filing_id`.
    :param all_metrics: All metrics.
    :param start_date: points modified from this date (inclusive).  If no time is specified all points from that date are returned.
    :param end_date: points modified until this date (exclusive).  If not time is specified point modified prior to this date are returned.
    :param XBRL_only: Only get data that appeared in an XBRL document.  If supplied with start_date and end_date it will filter by date_XBRL_confirmed, if a filing_id supplied it will filter by confirming_XBRL_filing_ID.
    :param all_modifications: Include data which was either written, modified, or confirmed as XBRL, in the specified date-range or filing_id.

    """
    if [
        bool(company_identifiers),
        bool(entire_universe),
        bool(filing_id),
    ].count(True) != 1:
        raise ValueError(
            "Must pass either company_identifiers, accession id, filing_id, or entire_universe=True"
        )
    if not any(
        [
            start_year,
            end_year,
            year,
            filing_id,
            all_history,
            period_type,
            start_date,
            end_date,
        ]
    ):
        raise ValueError("Need to specify a period qualifier")

    if filing_id and any(
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

    if pit_V2 and not point_in_time:
        raise ValueError("setting pit_V2 without point_in_time does not make sense")

    if all_modifications and not ((start_date or end_date) or filing_id):
        raise ValueError(
            "specifying all_modifications without date range or filing_id does not make sense"
        )

    date_range = None
    if start_date or end_date:
        date_range = DateRange(startDate=start_date, endDate=end_date)

    period_parameters = PeriodParameters(
        year=start_year,
        period=start_period,
        endYear=end_year,
        endPeriod=end_period,
        allHistory=all_history,
        periodType=period_type,
        useFiscalPeriod=use_fiscal_period,
        filingID=filing_id,
        dateRange=date_range,
        allModifications=all_modifications,
    )

    companies_parameters = CompaniesParameters(
        entireUniverse=entire_universe,
        companyIdentifiers=list(company_identifiers),
    )

    page_parameters = StandardizedParameters(
        metrics=metrics,
        includeTrace=include_trace,
        pointInTime=point_in_time,
        allFootnotes=all_footnotes,
        allFace=all_face,
        allNonGAAP=all_non_GAAP,
        allMetrics=all_metrics,
        pointInTimeV2=pit_V2,
        includePreliminary=True,
        XBRLOnly=XBRL_only,
    )

    payload = APIQueryParams(
        pageParameters=page_parameters,
        periodParameters=period_parameters,
        companiesParameters=companies_parameters,
    )
    response = _json_POST("mappedData", payload)
    return [
        StandardizedPoint(**d) for d in response
    ]  # It might be faster to use TypeAdapter(List[StandardizedPoint]).validate_python(response) but that signature changed between pydantic 1 and 2 so I am not doing it.


ORDERED_REGULAR_COLUMNS = [
    "ticker",
    "metric",
    "fiscal_year",
    "fiscal_period",
    "value",
    "CIK",
    "calendar_year",
    "calendar_period",
]
"""
Fields returned by non-point-in-time calls
"""

ORDERED_PIT_COLUMNS = [
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
    "date_modified",
    "date_XBRL_confirmed",
    "confirming_XBRL_filing_ID",
    "original_value",
]
"""
Fields returned by point-in-time calls
"""


def _build_data_frame(
    raw_data: Sequence[StandardizedPoint], point_in_time: bool
) -> "pd.DataFrame":
    """
    The order of the columns should remain constant
    """

    if not raw_data:
        return pd.DataFrame()
    columns = ORDERED_PIT_COLUMNS if point_in_time else ORDERED_REGULAR_COLUMNS
    data = pd.DataFrame(dict(r) for r in raw_data)
    if point_in_time:
        new_columns = list(set(data.columns) - set(columns))
        data = data.reindex(columns=columns + new_columns)
        data = data.drop(columns=["trace_facts"], errors="ignore")  # type: ignore
    else:
        data = data.reindex(columns=columns)
    return data


def standardized(
    company_identifiers: CompanyIdentifiers = [],
    metrics: Sequence[str] = [],
    fiscal_year: Optional[int] = None,
    fiscal_period: PeriodArgument = None,
    start_date: Optional[Union[datetime, date]] = None,
    end_date: Optional[Union[datetime, date]] = None,
    point_in_time: bool = False,
    filing_id: Optional[int] = None,
    pit_V2: Optional[bool] = None,
    XBRL_only: Optional[bool] = False,
    all_modifications: Optional[bool] = False,
):
    """Standardized Numeric Data.



    The data behind the multi-company page, https://www.calcbench.com/multi.

    Example https://github.com/calcbench/notebooks/blob/master/python_client_api_demo.ipynb

    :param company_identifiers: Tickers/CIK codes. eg. ['msft', 'goog', 'appl', '0000066740'].  If not specified get data for all companies.
    :param metrics: Standardized metrics.  Full list @ https://www.calcbench.com/home/standardizedmetrics eg. ['revenue', 'accountsreceivable'].  If not specified get all metrics.
    :param fiscal_year: Fiscal year for which to get data.  If not specified get all history.
    :param fiscal_period: Fiscal period for which to get data.  If not specified get all history.
    :param start_date:  Restrict to records modified on or after (inclusive) this date/datetime
    :param end_date:  Restric to records modified prior (exclusive) thie date/datetime
    :param point_in_time: Include timestamps when data was published and revision chains.
    :param filing_id: Filing ID for which to get data.  Get all of the data reported in this filing.
    :param pit_V2: Defaults to True, use point in time V2, this only makes sense when point_in_time = True.  This will go away at some point.
    :param XBRL_only: Only get data that appeared in an XBRL document.  If supplied with start_date and end_date it will filter by date_XBRL_confirmed, if a filing_id supplied it will filter by confirming_XBRL_filing_ID.
    :param all_modifications: Include data which was either written, modified, or confirmed as XBRL, in the specified date-range or filing_id.

    :return: Dataframe



    Standardized data with a timestamp when it was published by Calcbench.


    A record is returned for each filing in which a metric value changed (was revised).

    If the company files an 8-K with revenue = $100 then a week later files a 10-K with revenue = $100 one record will be returned for that period.  It will have preliminary=True and XBRL=True.

    If the company files an 8-K with revenue = $100 and then a week later files a 10-K with revenue = $200, two records will be returned.  One with revenue = $100, a revision_number=0, date_reported of 8-K, preliminary=True and XBRL=False.  You will see a second line with revenue = $200, a revision_number=1, and date_reported of the 10-K, preliminary=False and XBRL=False.

    If the value is revised in subsequent XBRL filings you will see a record for each filing with an incremented revision number.

        Columns:

    ticker
       Ticker of reporting company
    metric
       The metric name, see the definitions @ https://www.calcbench.com/home/standardizedmetrics
    fiscal_period
       fiscal_year-fiscal_period the fiscal period the value applies to.  Like "2020-0" or "2021-3".  0 indicates annual, 1,2,3,4 are quarters.
    date_reported (PIT only)
       Timestamp (EST) when Calcbench finished processing the filing from which this value was parsed.

       In some cases, particularly prior to 2015, this will be the filing date of the document as recorded by the SEC.  To exclude these points remove points where the hour is 0.

    value
       The value of the fact
    revision_number
       0 indicates an original, unrevised value for this fact. 1, 2, 3... indicates subsequent revisions to the fact value.  https://knowledge.calcbench.com/hc/en-us/search?utf8=%E2%9C%93&query=revisions&commit=Search
    preliminary
        True indicates the number was parsed from non-XBRL 8-K or press release from the wire
    XBRL
        Indicates the number was parsed from XBRL.

        The case where preliminary and XBRL are both true indicates the number was first parsed from a non-XBRL document then "confirmed" in an XBRL document.
    date_XBRL_confirmed
        Time at which the point was confirmed by a point from an XBRL filing.
        If the point originally came from an XBRL filing this will be the original write time.  For values originally appearing in press-release, this will be the date of the associated 10-K/Q.
        If this is null, for points post April 2023, the point has not been confirmed.
    period_start
       First day of the fiscal period for this fact
    period_end
       Last day of the fiscal period for this fact
    calendar_year
       The calendar year for this fact.  https://knowledge.calcbench.com/hc/en-us/articles/223267767-What-are-Calendar-Years-and-Periods-What-is-TTM-
    calendar_period
       The calendar period for this fact
    CIK
       SEC assigned Central Index Key for reporting company
    calcbench_entity_id
       Internal Calcbench identifier for reporting company
    filing_type
       The document type this fact came from, 10-K|Q, S-1 etc...
    date_modified (PIT only)
        The datetime Calcbench wrote/modified this value.

        Post November 2022 if this differs from the date_reported the fact was modified by Calcbench subsequent to the filing first being processed.
    fling_accession_number
        Accession number as assigned by the SEC for the filing from which this value came.
    trace_url
        URL for a page showing the source document for this value.
    original_value (PIT only)
        The value that Calcbench extracted when it first processed the filing.

        Post November 2022, if this differs from the value Calcbench the fact was modified by Calcbench subsequent to the filing first being processed.
    standardized_id
        A unique identifier Calcbench assigns to each standardized value.
    confirming_XBRL_filing_ID
        The filing_id of the XBRL document which contained this value.
    date_downloaded
        The timestamp on your computer when you downloaded this data.



    Usage::

      >>> d = calcbench.standardized(company_identifiers=['msft'],
      >>>                                 point_in_time=True,)
      >>> )
      >>> # Put the data in a format amiable to arithmetic on columns
      >>> d = calcbench.standardized(company_identifiers=['msft', 'orcl'], metrics=['StockholdersEquity', 'NetIncome'])
      >>> d = d.unstack("metric")["value"]
      >>> return_on_equity = d['NetIncome'] / d['StockholdersEquity']

    """

    company_identifiers = list(company_identifiers)
    if point_in_time and pit_V2 is None:
        pit_V2 = True
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
        include_trace=True,
        pit_V2=pit_V2,
        start_date=start_date,
        end_date=end_date,
        XBRL_only=XBRL_only,
        all_modifications=all_modifications,
    )

    data = _build_data_frame(data, point_in_time=point_in_time)
    if data.empty:
        return data
    data["fiscal_period"] = (
        data["fiscal_year"].astype(str) + "-" + data["fiscal_period"].astype(str)
    ).astype("string")
    data = data.drop("fiscal_year", axis=1)

    if "calendar_period" in data.columns:
        data.calendar_period = data.calendar_period.astype(period_number)

    if "calendar_period" in data.columns:
        data.calendar_period = data.calendar_period.astype(period_number)

    for string_column in [
        "ticker",
        "metric",
        "CIK",
        "filing_type",
        "filing_accession_number",
        "trace_url",
    ]:
        if string_column in data.columns:
            data[string_column] = data[string_column].astype("string")

    index_columns = [
        "ticker",
        "metric",
        "fiscal_period",
    ]
    if point_in_time:
        index_columns = index_columns + ["date_reported"]
        data["date_downloaded"] = datetime.now()
        data["preliminary_only"] = data["preliminary"] & ~data["XBRL"]
    data = data.set_index(index_columns)
    data = data.sort_index()
    return data
