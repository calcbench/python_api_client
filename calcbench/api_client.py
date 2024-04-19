"""
Created on Mar 14, 2015

@author: Andrew Kittredge
@copyright: Calcbench, Inc.
@contact: andrew@calcbench.com
"""

import dataclasses
import json
import logging
import os
from datetime import datetime
from functools import wraps
from typing import (
    TYPE_CHECKING,
    Callable,
    Dict,
    Iterable,
    Optional,
    Tuple,
    Union,
    cast,
)
import getpass
import warnings
import subprocess
import sys


if TYPE_CHECKING:
    # https://github.com/microsoft/pyright/issues/1358
    from typing import TypedDict
else:
    try:
        from typing import TypedDict
    except ImportError:
        from typing_extensions import TypedDict

from requests.sessions import Session
from requests import HTTPError

from calcbench.api_query_params import APIQueryParams


import requests
from requests import RequestException

logger = logging.getLogger(__name__)

__version__ = "13.0.0"

USER_AGENT = f"cb_python_api_client/{__version__}"


class _SESSION_VARIABLES(TypedDict):
    calcbench_user_name: Optional[str]
    calcbench_password: Optional[str]
    api_url_base: str
    logon_url: str
    domain: str
    session: Optional[Session]
    ssl_verify: bool
    timeout: int
    enable_backoff: bool
    proxies: Dict[str, str]
    backoff_giveup: Optional[Callable[[RequestException], bool]]


_SESSION_STUFF: _SESSION_VARIABLES = {
    "calcbench_user_name": None,
    "calcbench_password": None,
    "api_url_base": "https://www.calcbench.com/api/{0}",
    "logon_url": "https://www.calcbench.com/account/LogOnAjax",
    "domain": "https://www.calcbench.com/{0}",
    "ssl_verify": True,
    "session": None,
    "timeout": 60 * 20,  # twenty minute content request timeout, by default
    "enable_backoff": False,
    "proxies": {},
    "backoff_giveup": None,
}

KEYRING_SERVICE_NAME = "calcbench_api"

USERNAME_ENVIRONMENT_VARIABLE = "CALCBENCH_USERNAME"
PASSWORD_ENVIRONMENT_VARIABLE = "CALCBENCH_PASSWORD"


def _get_credentials() -> Tuple[str, str]:
    """
    Used to store credentials as environment variables.  Now we use keyring

    """
    session_user_name = _SESSION_STUFF.get("calcbench_user_name")
    session_password = _SESSION_STUFF.get("calcbench_password")
    if session_user_name and session_password:
        return (session_user_name, session_password)
    keyring_found = False
    try:
        import keyring

        keyring_found = True
        logger.info("found keyring module")
    except ImportError:
        logger.info("did not find keyring module")
    else:
        try:
            keychain_credentials = keyring.get_credential(KEYRING_SERVICE_NAME, None)
        except Exception as e:
            warnings.warn(f"Exception getting credentials from keyring {e}")
        else:
            if keychain_credentials:
                user_name = cast(str, keychain_credentials.username)
                password = cast(str, keychain_credentials.password)
                logger.info(f"found credentials for {user_name} in keyring")
                _SESSION_STUFF["calcbench_user_name"] = user_name
                _SESSION_STUFF["calcbench_password"] = password
                return (user_name, password)
    environment_user_name = os.environ.get(USERNAME_ENVIRONMENT_VARIABLE)
    environment_password = os.environ.get(PASSWORD_ENVIRONMENT_VARIABLE)
    if environment_user_name and environment_password:
        _SESSION_STUFF["calcbench_user_name"] = environment_user_name
        _SESSION_STUFF["calcbench_password"] = environment_password
        logger.info(
            f"found credentials for {environment_user_name} in environment variables"
        )
        return (cast(str, environment_user_name), cast(str, environment_password))
    else:
        user_name = input("Calcbench username/email::")
        password = getpass.getpass("Calcbench password::")

        _SESSION_STUFF["calcbench_user_name"] = user_name
        _SESSION_STUFF["calcbench_password"] = password
        store_in_keychain = (
            input(
                f"Store credentials in keychain? (yes/no).  {'' if keyring_found else 'Will attempt to install the keyring package.'}"
            ).lower()[0]
            == "y"
        )

        if store_in_keychain:
            if not keyring_found:
                try:
                    subprocess.check_call(
                        [sys.executable, "-m", "pip", "install", "keyring"]
                    )
                except Exception as e:
                    logger.error("exception installing keyring module")
                else:
                    logger.info("successfully installed keyring package")
                    keyring_found = True
            if keyring_found:
                import keyring

                try:
                    keyring.set_password(
                        KEYRING_SERVICE_NAME, username=user_name, password=password
                    )

                except Exception as e:
                    warnings.warn(f"Exception saving credentials to keyring {e}")
                else:
                    logger.info("saved credentials to keyring")
        else:
            print(
                f'To avoid this prompt: save credentials to keychain, call "cb.set_credentials()" or set the "{USERNAME_ENVIRONMENT_VARIABLE}" and "{PASSWORD_ENVIRONMENT_VARIABLE}" environment variable'
            )
        return (user_name, password)


def _calcbench_session() -> Session:
    session = _SESSION_STUFF.get("session")
    if not session:
        user_name, password = _get_credentials()

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
            _SESSION_STUFF["calcbench_user_name"] = None
            _SESSION_STUFF["calcbench_password"] = None
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


def _add_backoff(f):
    @wraps(f)
    def wrapper(*args, **kwargs):
        if _SESSION_STUFF["enable_backoff"]:
            import backoff

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


HEADERS = {
    "content-type": "application/json",
    "User-Agent": USER_AGENT,
}


@_add_backoff
def _json_POST(end_point: str, payload: Union[dict, APIQueryParams]):
    session = _calcbench_session()
    url = _SESSION_STUFF["api_url_base"].format(end_point)
    data = (
        payload.json(exclude_unset=True, exclude_none=True)
        if isinstance(payload, APIQueryParams)
        else json.dumps(payload)
    )
    logger.debug(f"posting to {url}, {data}")
    start = datetime.now()
    response = session.post(
        url,
        data=data,
        headers=HEADERS,
        verify=_SESSION_STUFF["ssl_verify"],
        timeout=_SESSION_STUFF["timeout"],
    )
    try:
        response.raise_for_status()
    except requests.exceptions.HTTPError as e:
        logger.exception("Exception {0}, {1}".format(url, payload))
        raise e
    response_data = response.json()
    logger.debug(f"In {datetime.now() - start} got, {response.text[:1000]}")
    return response_data


@_add_backoff
def _json_GET(path: str, params: dict = {}):
    url = _SESSION_STUFF["domain"].format(path)
    response = _calcbench_session().get(
        url,
        params=params,
        headers=HEADERS,
        verify=_SESSION_STUFF["ssl_verify"],
        timeout=_SESSION_STUFF["timeout"],
    )
    try:
        response.raise_for_status()
    except requests.exceptions.HTTPError as e:
        logger.exception("Exception {0}, {1}".format(url, params))
        raise e
    return response.json()


@_add_backoff
def set_credentials(cb_username: str, cb_password: str):
    """Set your calcbench credentials.

    Call this before any other Calcbench functions.

    Alternatively set the ``CALCBENCH_USERNAME`` and ``CALCBENCH_PASSWORD`` environment variables OR set the calcbench_api credentials on your Keychain|Secret Service|Windows Credential Locker

    :param str cb_username: Your calcbench.com email address
    :param str cb_password: Your calcbench.com password

    Usage::

      >>> calcbench.set_credentials("andrew@calcbench.com", "NotMyRealPassword")

    """
    _SESSION_STUFF["calcbench_user_name"] = cb_username
    _SESSION_STUFF["calcbench_password"] = cb_password
    _calcbench_session()  # Make sure credentials work.


def enable_backoff(
    backoff_on: bool = True,
    giveup: Callable[[RequestException], bool] = lambda e: isinstance(e, HTTPError)
    and e.response.status_code == 404,
):
    """Re-try failed requests with exponential back-off

    Requires the backoff package. ``pip install backoff``

    If processes make many requests, failures are inevitable.  Call this to retry failed requests.

    By default gives up immediately if the server returns 404

    :param backoff_on: toggle backoff
    :param giveup: function that handles exception and decides whether to continue or not.

    Usage::
        >>> calcbench.enable_backoff(giveup=lambda e: e.response and e.response.status_code == 404)

    """
    if backoff_on:
        try:
            import backoff
        except ImportError:
            print("backoff package not found, `pip install backoff`")
            raise
        _SESSION_STUFF["backoff_giveup"] = giveup

    _SESSION_STUFF["enable_backoff"] = backoff_on


def set_proxies(proxies: Dict[str, str]):
    """
    Set proxies used for requests.  See https://requests.readthedocs.io/en/master/user/advanced/#proxies

    """
    _SESSION_STUFF["proxies"] = proxies


def set_field_values(dataclass, kwargs: dict, date_columns: Iterable[str] = []):
    names = set([f.name for f in dataclasses.fields(dataclass)])
    for k, v in kwargs.items():
        if k in names:
            if k in date_columns:
                v = _try_parse_timestamp(v)
            setattr(dataclass, k, v)


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


def html_diff(html_1, html_2):
    """Diff two pieces of html and return a html diff"""
    return _json_POST("textDiff", {"html1": html_1, "html2": html_2})
