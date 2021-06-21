"""
Created on Mar 14, 2015

@author: Andrew Kittredge
@copyright: Calcbench, Inc.
@contact: andrew@calcbench.com
"""

import json
import logging
import os
from datetime import datetime
from enum import Enum, IntEnum
from functools import wraps
from typing import (
    Callable,
    Dict,
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
    import pandas as pd
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


def set_credentials(cb_username: str, cb_password: str):
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


class PeriodType(str, Enum):
    Annual = "annual"
    Quarterly = "quarterly"
    Combined = "combined"
    TrailingTwelveMonths = "TTM"


class Period(IntEnum):
    Annual = 0
    Q1 = 1
    Q2 = 2
    Q3 = 3
    Q4 = 4


PeriodArgument = Optional[Union[Period, Literal[0, 1, 2, 3, 4]]]


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


def _try_parse_timestamp(timestamp: str):
    """
    We did not always have milliseconds
    """
    if not timestamp:
        return None
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
