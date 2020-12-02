"""
Created on Mar 14, 2015

@author: Andrew Kittredge
@copyright: Calcbench, Inc.
@contact: andrew@calcbench.com
"""

import json
import logging
import os
import warnings
from datetime import date, datetime
from enum import Enum, IntEnum
from functools import wraps
from typing import (
    Callable,
    Dict,
    Iterable,
    Optional,
    Sequence,
    Union,
)

from requests.sessions import Session

try:
    from typing import Literal
except ImportError:
    from typing_extensions import Literal

import requests

logger = logging.getLogger(__name__)


try:
    import numpy as np
    import pandas as pd

    period_number = pd.api.types.CategoricalDtype(  # type: ignore
        categories=[1, 2, 3, 4, 5, 6, 0], ordered=True
    )  # So annual is last in sorting.  5 and 6 are first half and 3QCUM.

except ImportError:
    "Can't find pandas, won't be able to use the functions that return DataFrames."
    pass


_SESSION_STUFF = {
    "calcbench_user_name": os.environ.get("CALCBENCH_USERNAME"),
    "calcbench_password": os.environ.get("CALCBENCH_PASSWORD"),
    "api_url_base": "https://www.calcbench.com/api/{0}",
    "logon_url": "https://www.calcbench.com/account/LogOnAjax",
    "domain": "https://www.calcbench.com/{0}",
    "ssl_verify": True,
    "session": None,
    "timeout": 60 * 20,  # twenty minute content request timeout, by default
    "enable_backoff": False,
    "proxies": None,
}


def _calcbench_session() -> Session:
    session = _SESSION_STUFF.get("session")
    if not session:
        user_name = _SESSION_STUFF.get("calcbench_user_name")
        password = _SESSION_STUFF.get("calcbench_password")
        if not (user_name and password):
            import getpass

            user_name = input(
                'Calcbench username/email. Set the "calcbench_user_name" environment variable or call "set_credentials" to avoid this prompt::'
            )
            password = getpass.getpass(
                'Calcbench password.  Set the "calcbench_password" enviroment variable to avoid this prompt::'
            )
        session = requests.Session()
        if _SESSION_STUFF.get("proxies"):
            session.proxies.update(_SESSION_STUFF["proxies"])
        r = session.post(
            _SESSION_STUFF["logon_url"],
            {"email": user_name, "password": password, "rememberMe": "true"},
            verify=_SESSION_STUFF["ssl_verify"],
            timeout=_SESSION_STUFF["timeout"],
        )
        r.raise_for_status()
        if r.text != "true":
            raise ValueError(
                "Incorrect Credentials, use the email and password you use to login to Calcbench."
            )
        else:
            _SESSION_STUFF["session"] = session
    return session


def _rig_for_testing(domain="localhost:444", suppress_http_warnings=True):
    _SESSION_STUFF["api_url_base"] = "https://" + domain + "/api/{0}"
    _SESSION_STUFF["logon_url"] = "https://" + domain + "/account/LogOnAjax"
    _SESSION_STUFF["domain"] = "https://" + domain + "/{0}"
    _SESSION_STUFF["ssl_verify"] = False
    _SESSION_STUFF["session"] = None
    if suppress_http_warnings:
        from requests.packages.urllib3.exceptions import InsecureRequestWarning

        requests.packages.urllib3.disable_warnings(InsecureRequestWarning)  # type: ignore


PeriodType = Literal["annual", "quarterly"]


CentralIndexKey = Union[str, int]
Ticker = str
CalcbenchCompanyIdentifier = int
CompanyIdentifier = Union[Ticker, CentralIndexKey, CalcbenchCompanyIdentifier]
CompanyIdentifiers = Sequence[CompanyIdentifier]


def _add_backoff(f):
    @wraps(f)
    def wrapper(*args, **kwargs):
        if _SESSION_STUFF["enable_backoff"]:
            backoff = _SESSION_STUFF["backoff_package"]
            return backoff.on_exception(
                backoff.expo,
                requests.exceptions.RequestException,
                max_tries=8,
                logger=logger,
                giveup=_SESSION_STUFF["backoff_giveup"],
            )(f)(*args, **kwargs)
        else:
            return f(*args, **kwargs)

    return wrapper


@_add_backoff
def _json_POST(end_point: str, payload: dict):
    url = _SESSION_STUFF["api_url_base"].format(end_point)
    logger.debug(f"posting to {url}, {payload}")

    response = _calcbench_session().post(
        url,
        data=json.dumps(payload),
        headers={"content-type": "application/json"},
        verify=_SESSION_STUFF["ssl_verify"],
        timeout=_SESSION_STUFF["timeout"],
    )
    try:
        response.raise_for_status()
    except requests.exceptions.HTTPError as e:
        logger.exception("Exception {0}, {1}".format(url, payload))
        raise e
    response_data = response.json()
    logger.debug(f"{response_data}")
    return response_data


@_add_backoff
def _json_GET(path: str, params: dict = {}):
    url = _SESSION_STUFF["domain"].format(path)
    response = _calcbench_session().get(
        url,
        params=params,
        headers={"content-type": "application/json"},
        verify=_SESSION_STUFF["ssl_verify"],
        timeout=_SESSION_STUFF["timeout"],
    )
    try:
        response.raise_for_status()
    except requests.exceptions.HTTPError as e:
        logger.exception("Exception {0}, {1}".format(url, params))
        raise e
    return response.json()


def set_credentials(cb_username, cb_password):
    """Set your calcbench credentials.

    Call this before any other Calcbench functions.

    Alternatively set the ``CALCBENCH_USERNAME`` and ``CALCBENCH_PASSWORD`` environment variables

    :param str cb_username: Your calcbench.com email address
    :param str cb_password: Your calcbench.com password

    Usage::

      >>> calcbench.set_credentials("andrew@calcbench.com", "NotMyRealPassword")

    """
    _SESSION_STUFF["calcbench_user_name"] = cb_username
    _SESSION_STUFF["calcbench_password"] = cb_password
    _calcbench_session()  # Make sure credentials work.


def enable_backoff(
    backoff_on: bool = True, giveup: Callable[[Exception], bool] = lambda e: False
):
    """Re-try failed requests with exponential back-off

    Requires the backoff package. ``pip install backoff``

    If processes make many requests, failures are inevitable.  Call this to retry failed requests.

    :param backoff_on: toggle backoff
    :param giveup: function that handles exception and decides whether to continue or not.
    Usage::
        >>> calcbench.enable_backoff(giveup=lambda e: e.response.status_code == 404)
    """
    if backoff_on:
        try:
            import backoff
        except ImportError:
            print("backoff package not found, `pip install backoff`")
            raise

        _SESSION_STUFF["backoff_package"] = backoff

    _SESSION_STUFF["enable_backoff"] = backoff_on
    _SESSION_STUFF["backoff_giveup"] = giveup


def set_proxies(proxies: Dict[str, str]):
    """
    Set proxies used for requests.  See https://requests.readthedocs.io/en/master/user/advanced/#proxies

    """
    _SESSION_STUFF["proxies"] = proxies


class CompanyIdentifierScheme(str, Enum):
    Ticker = "ticker"
    CentralIndexKey = "CIK"


class Period(IntEnum):
    Annual = 0
    Q1 = 1
    Q2 = 2
    Q3 = 3
    Q4 = 4


PeriodArgument = Optional[Union[Period, Literal[0, 1, 2, 3, 4]]]


def normalized_dataframe(
    company_identifiers: CompanyIdentifiers = [],
    metrics: Sequence[str] = [],
    start_year: int = None,
    start_period: PeriodArgument = None,
    end_year: int = None,
    end_period: PeriodArgument = None,
    entire_universe: bool = False,
    filing_accession_number: int = None,
    point_in_time: bool = False,
    year: int = None,
    period: PeriodArgument = None,
    all_history: bool = False,
    period_type: PeriodType = None,
    trace_hyperlinks: bool = False,
    use_fiscal_period: bool = False,
    company_identifier_scheme: CompanyIdentifierScheme = CompanyIdentifierScheme.Ticker,
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
    :param filing_accession_number: Filing Accession ID from the SEC's Edgar system.
    :param year: Get data for a single year, defaults to annual data.
    :param period_type: Either "annual" or "quarterly".
    :return: Dataframe with the periods as the index and columns indexed by metric and ticker

    Usage::

      >>> calcbench.standardized_data(company_identifiers=['msft', 'goog'], metrics=['revenue', 'assets'], all_history=True, period_type='annual')

    """

    if all_history and not period_type:
        raise ValueError("For all history you must specify a period_type")
    data = normalized_raw(
        company_identifiers=list(company_identifiers),
        metrics=metrics,
        start_year=start_year,
        start_period=start_period,
        end_year=end_year,
        end_period=end_period,
        entire_universe=entire_universe,
        point_in_time=point_in_time,
        filing_accession_number=filing_accession_number,
        year=year,
        period=period,
        all_history=all_history,
        period_type=period_type,
        use_fiscal_period=use_fiscal_period,
    )
    if not data:
        warnings.warn("No data found")
        return pd.DataFrame()

    quarterly = (start_period and end_period) or period_type == "quarterly"
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
                value = '=HYPERLINK("https://www.calcbench.com/benchmark/traceValueExcelV2?metric={metric}&cid={ticker}&year={fiscal_year}&period={fiscal_period}&useFiscalPeriod=True&showLinks=true", {value})'.format(
                    **d
                )
            d["value"] = value
        except (ValueError, KeyError):
            pass
        metrics_found.add(d["metric"])

    missing_metrics = set(metrics) - metrics_found
    if missing_metrics:
        warnings.warn("Did not find metrics {0}".format(missing_metrics))
    data = pd.DataFrame(data)
    data.set_index(
        keys=[f"{company_identifier_scheme}", "metric", "period"], inplace=True  # type: ignore
    )  # type: ignore
    try:
        data = data.unstack("metric")["value"]  # type: ignore
    except ValueError as e:
        if str(e) == "Index contains duplicate entries, cannot reshape":
            duplicates = data[data.index.duplicated()]["value"]  # type: ignore
            logger.error("Duplicate values \n {0}".format(duplicates))
        raise

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


def _build_quarter_period(data_point, use_fiscal_period):
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


def _build_annual_period(data_point, use_fiscal_period):
    data_point.pop("fiscal_period" if use_fiscal_period else "calendar_period")
    return pd.Period(  # type: ignore
        year=data_point.pop("fiscal_year" if use_fiscal_period else "calendar_year"),
        freq="a",
    )


normalized_data = normalized_dataframe  # used to call it normalized_data.
standardized_data = normalized_dataframe  # Now it's called standardized data


def normalized_raw(
    company_identifiers: CompanyIdentifiers = [],
    metrics: Iterable[
        str
    ] = [],  # type str[] Full list of metrics is @ https://www.calcbench.com/home/standardizedmetrics
    start_year: int = None,
    start_period: PeriodArgument = None,
    end_year: int = None,
    end_period: PeriodArgument = None,
    entire_universe=False,
    filing_accession_number=None,
    point_in_time: bool = False,
    include_trace: bool = False,
    update_date: date = None,
    all_history=False,
    year: int = None,
    period: PeriodArgument = None,
    period_type: PeriodType = None,
    include_preliminary: bool = False,
    use_fiscal_period: bool = False,
):
    """
    Standardized data.

    Get normalized data from Calcbench.  Each point is normalized by economic concept and time period.

    Args:
        company_identifiers: a sequence of tickers (or CIK codes), eg ['msft', 'goog', 'appl']
        metrics: a sequence of metrics, see the full list @ https://www.calcbench.com/home/standardizedmetrics eg. ['revenue', 'accountsreceivable']
        start_year: first year of data
        start_period: first quarter to get, for annual data pass 0, for quarters pass 1, 2, 3, 4
        end_year: last year of data
        end_period: last_quarter to get, for annual data pass 0, for quarters pass 1, 2, 3, 4
        entire_universe: Get data for all companies, this can take a while, talk to Calcbench before you do this in production.
        accession_id: Filing Accession ID from the SEC's Edgar system.
        include_trace: Include the facts used to calculate the normalized value.
        year: Get data for a single year, defaults to annual data.
        period_type: Either "annual" or "quarterly"
        include_preliminary: Include data from non-XBRL 8-Ks and press releases.

    Returns:
        A list of dictionaries with keys ['ticker', 'calendar_year', 'calendar_period', 'metric', 'value'].

        For example
            [
                {
                    "ticker": "MSFT",
                    "calendar_year": 2010,
                    "calendar_period": 1,
                    "metric": "revenue",
                    "value": 14503000000
                },
                {
                    "ticker": "MSFT",
                    "calendar_year": 2010,
                    "calendar_period": 2,
                    "metric": "revenue",
                    "value": 16039000000
                },
            ]
    """
    if [
        bool(company_identifiers),
        bool(entire_universe),
        bool(filing_accession_number),
    ].count(True) != 1:
        raise ValueError(
            "Must pass either company_identifiers and accession id or entire_universe=True"
        )

    if filing_accession_number and any(
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
            "Accession IDs are specific to a filing, no other qualifiers make sense."
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

    if period_type and period_type not in ("annual", "quarterly"):
        raise ValueError('period_type must be either "annual" or "quarterly"')

    if include_preliminary and not point_in_time:
        raise ValueError("include_preliminary only works for PIT")

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
    payload = {
        "pageParameters": {
            "metrics": metrics,
            "includeTrace": include_trace,
            "pointInTime": point_in_time,
            "includePreliminary": include_preliminary,
        },
        "periodParameters": {
            "year": start_year,
            "period": start_period,
            "endYear": end_year,
            "endPeriod": end_period,
            "allHistory": all_history,
            "updateDate": update_date and update_date.isoformat(),
            "periodType": period_type,
            "useFiscalPeriod": use_fiscal_period,
        },
        "companiesParameters": {
            "entireUniverse": entire_universe,
            "companyIdentifiers": list(company_identifiers),
        },
    }

    return _json_POST("mappedData", payload)


def point_in_time(
    company_identifiers: CompanyIdentifiers = [],
    all_footnotes: bool = False,
    update_date: date = None,
    metrics: Iterable[str] = [],
    all_history: bool = False,
    entire_universe: bool = False,
    start_year: int = None,
    start_period: PeriodArgument = None,
    end_year: int = None,
    end_period: PeriodArgument = None,
    period_type: PeriodType = None,
    use_fiscal_period: bool = False,
    include_preliminary: bool = False,
    all_face: bool = False,
    include_xbrl: bool = True,
    accession_id: int = None,
    include_trace: bool = False,
) -> "pd.DataFrame":
    """Point-in-Time Data

    Standardized data with a timestamp when it was published by Calcbench

    :param update_date: The date on which the data was received, this does not work prior to ~2016, use all_history to get historical data then use update_date to get updates.
    :param accession_id: Unique identifier for the filing for which to recieve data.  Pass this to recieve data for one filing.  Same as filing_id in filings objects
    :param all_face: Retrieve all of the points from the face financials, income/balance/statement of cash flows
    :param all_footnotes: Retrive all of the points from the footnotes to the financials
    :param include_preliminary: Include facts from non-XBRL earnings press-releases and 8-Ks.
    :param include_xbrl: Include facts from XBRL 10-K/Qs.
    :return: DataFrame of facts

    Columns:


    value
       The value of the fact
    revision_number
       0 indicates an original, unrevised value for this fact. 1, 2, 3... indicates subsequent revisions to the fact value.  https://knowledge.calcbench.com/hc/en-us/search?utf8=%E2%9C%93&query=revisions&commit=Search
    period_start
       First day of the fiscal period for this fact
    period_end
       Last day of the fiscal period for this fact
    date_reported
       Timestamp when Calcbench published this fact
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
       >>> calcbench.point_in_time(company_identifiers=["msft", "goog"], all_history=True, all_face=True, all_footnotes=True)

    .. _Example: https://github.com/calcbench/notebooks/blob/master/standardized_numeric_point_in_time.ipynb


    """

    data = mapped_raw(
        company_identifiers=company_identifiers,
        all_footnotes=all_footnotes,
        point_in_time=True,
        update_date=update_date,
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
        all_face=all_face,
        include_xbrl=include_xbrl,
        accession_id=accession_id,
        include_trace=include_trace,
    )
    if not data:
        return pd.DataFrame()
    if include_trace:
        for point in data:
            trace_facts = point.pop("trace_facts", None)
            if trace_facts:
                fact_id = trace_facts[0]["fact_id"]
                point[
                    "trace_url"
                ] = f"https://calcbench.com/benchmark/traceValueExcelV2?nonXBRLFactIDs={fact_id}"
    data = pd.DataFrame(data)

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
    return data.sort_values(sort_columns).reset_index(drop=True)


def mapped_raw(
    company_identifiers=[],
    all_footnotes=False,
    point_in_time=False,
    update_date=None,
    metrics=[],
    all_history=False,
    entire_universe=False,
    start_year=None,
    end_year=None,
    start_period=None,
    end_period=None,
    period_type=None,
    use_fiscal_period=False,
    include_preliminary=False,
    all_face=False,
    include_xbrl=True,
    accession_id=None,
    include_trace=False,
) -> "pd.DataFrame":
    if update_date:
        warnings.warn(
            "Request updates by accession_id rather than update date",
            DeprecationWarning,
        )
    if accession_id and update_date:
        raise ValueError("Specifying accession_id and update_date is redundant.")

    payload = {
        "companiesParameters": {
            "companyIdentifiers": list(company_identifiers),
            "entireUniverse": entire_universe,
        },
        "pageParameters": {
            "pointInTime": point_in_time,
            "allFootnotes": all_footnotes,
            "allface": all_face,
            "metrics": metrics,
            "includePreliminary": include_preliminary,
            "includeTrace": include_trace,
        },
    }
    period_parameters = {
        "allHistory": all_history,
        "year": start_year,
        "endYear": end_year,
        "period": start_period,
        "endPeriod": end_period,
        "periodType": period_type,
        "useFiscalPeriod": use_fiscal_period,
        "includeXBRL": include_xbrl,
        "accessionID": accession_id,
    }
    if update_date:
        period_parameters["updateDate"] = update_date.isoformat()
    payload["periodParameters"] = period_parameters
    return _json_POST("mappedData", payload)


def face_statement(
    company_identifier,
    statement_type,
    period_type="annual",
    all_history=False,
    descending_dates=False,
):
    """Face Statements.

    face statements as reported by the filing company


    :param string company_identifier: a ticker or a CIK code, eg 'msft'
    :param string statement_type: one of ('income', 'balance', 'cash', 'change-in-equity', 'comprehensive-income')
    :param string period_type: annual|quarterly|cummulative|combined
    :param string all_periods: get all history or only the last four, True or False.
    :param bool descending_dates: return columns in oldest -> newest order.

    :rtype: object

    Returns:
    An object with columns and line items lists.  The columns have fiscal_period, period_start, period_end and instant values.
    The line items have label, local_name (the XBRL tag name), tree_depth (the indent amount), is_subtotal (whether or not the line item is computed from above metrics) and facts.
    The facts are in the same order as the columns and have fact_ids (an internal Calcbench ID), unit_of_measure (USD etc), effective_value (the reported value), and format_type.


    Usage::
        >>> calcbench.face_statement('msft', 'income')

    """
    url = _SESSION_STUFF["api_url_base"].format("asReported")
    payload = {
        "companyIdentifier": company_identifier,
        "statementType": statement_type,
        "periodType": period_type,
        "allPeriods": all_history,
        "descendingDates": descending_dates,
    }
    response = _calcbench_session().get(
        url,
        params=payload,
        headers={"content-type": "application/json"},
        verify=_SESSION_STUFF["ssl_verify"],
    )
    response.raise_for_status()
    data = response.json()
    return data


as_reported_raw = face_statement


def dimensional_raw(
    company_identifiers=None,
    metrics=[],
    start_year=None,
    start_period=None,
    end_year=None,
    end_period=None,
    period_type="annual",
):
    """Segments and Breakouts

    The data behind the breakouts/segment page, https://www.calcbench.com/breakout.

    :param sequence company_identifiers: Tickers/CIK codes. eg. ['msft', 'goog', 'appl', '0000066740']
    :param sequence metrics: list of dimension tuple strings, get the list @ https://www.calcbench.com/api/availableBreakouts, pass in the "databaseName"
    :param int start_year: first year of data to get
    :param int start_period: first period of data to get.  0 for annual data, 1, 2, 3, 4 for quarterly data.
    :param int end_year: last year of data to get
    :param int end_period: last period of data to get. 0 for annual data, 1, 2, 3, 4 for quarterly data.
    :param str period_type: 'quarterly' or 'annual', only applicable when other period data not supplied.
    :return: A list of points.  The points correspond to the lines @ https://www.calcbench.com/breakout.  For each requested metric there will be a the formatted value and the unformatted value denote bya  _effvalue suffix.  The label is the dimension label associated with the values.
    :rtype: sequence

    Usage::
      >>> cb.dimensional_raw(company_identifiers=['fdx'], metrics=['OperatingSegmentRevenue'], start_year=2018)

    """
    if len(metrics) == 0:
        raise (ValueError("Need to supply at least one breakout."))
    if period_type not in ("annual", "quarterly"):
        raise (ValueError("period_type must be in ('annual', 'quarterly')"))

    payload = {
        "companiesParameters": {
            "entireUniverse": len(company_identifiers) == 0,
            "companyIdentifiers": company_identifiers,
        },
        "periodParameters": {
            "year": end_year or start_year,
            "period": start_period,
            "endYear": start_year,
            "periodType": period_type,
            "asOriginallyReported": False,
        },
        "pageParameters": {
            "metrics": metrics,
            "dimensionName": "Segment",
            "AsOriginallyReported": False,
        },
    }
    return _json_POST("dimensionalData", payload)


def tag_contents(accession_id, block_tag_name):
    payload = {"accession_ids": accession_id, "block_tag_name": block_tag_name}
    json = _json_GET("query/disclosuresByTag", payload)
    return json[0]["blobs"][0]


def company_disclosures(ticker, period=None, year=None, statement_type=None):
    payload = {"companyIdentifier": ticker}
    if period:
        payload["period"] = period
    if year:
        payload["year"] = year
    if statement_type:
        payload["statementType"] = statement_type
    url = _SESSION_STUFF["api_url_base"].format("companyDisclosures")
    r = _calcbench_session().get(
        url, params=payload, verify=_SESSION_STUFF["ssl_verify"]
    )
    r.raise_for_status()
    return r.json()


def disclosure_text(network_id):
    url = _SESSION_STUFF["api_url_base"].format("disclosure")
    r = _calcbench_session().get(
        url, params={"networkID": network_id}, verify=_SESSION_STUFF["ssl_verify"]
    )
    r.raise_for_status()
    return r.json()


def business_combinations(company_identifiers):
    payload = {
        "companiesParameters": {"companyIdentifiers": company_identifiers},
        "pageParameters": {},
    }
    period_parameters = {}
    payload["periodParameters"] = period_parameters
    return _json_POST("businessCombinations", payload)


def _try_parse_timestamp(timestamp):
    """
    We did not always have milliseconds
    """
    try:
        timestamp = timestamp[:26]  # .net's milliseconds are too long
        return datetime.strptime(timestamp, "%Y-%m-%dT%H:%M:%S.%f")
    except ValueError:
        return datetime.strptime(timestamp, "%Y-%m-%dT%H:%M:%S")


def document_types():
    url = _SESSION_STUFF["api_url_base"].format("documentTypes")
    r = _calcbench_session().get(url, verify=_SESSION_STUFF["ssl_verify"])
    r.raise_for_status()
    return r.json()


def html_diff(html_1, html_2):
    """Diff two pieces of html and return a html diff"""
    return _json_POST("textDiff", {"html1": html_1, "html2": html_2})


def press_release_raw(
    company_identifiers,
    year,
    period,
    match_to_previous_period=False,
    standardize_beginning_of_period=False,
):
    payload = {
        "companiesParameters": {"companyIdentifiers": list(company_identifiers)},
        "periodParameters": {"year": year, "period": period},
        "pageParameters": {
            "matchToPreviousPeriod": match_to_previous_period,
            "standardizeBOPPeriods": standardize_beginning_of_period,
        },
    }
    return _json_POST("pressReleaseData", payload)


rawXBRLEndPoint = "rawXBRLData"
rawNonXBRLEndPoint = "rawNonXBRLData"


def raw_data(
    company_identifiers=[], entire_universe=False, clauses=[], end_point=rawXBRLEndPoint
):
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
    if end_point not in (rawXBRLEndPoint, rawNonXBRLEndPoint):
        raise ValueError(
            f"end_point must be either {rawXBRLEndPoint} or {rawNonXBRLEndPoint}"
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
    company_identifiers=[], entire_universe=False, clauses=[], end_point=rawXBRLEndPoint
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
    if end_point == rawXBRLEndPoint:
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
