"""
The "public" properties on the cb module

"""
__version__ = "8.0.0"
from datetime import datetime
import logging
from .api_client import (
    enable_backoff,
    html_diff,
    set_credentials,
    set_proxies,
)

from .disclosures import (
    disclosure_dataframe,
    disclosure_search,
)

# disclosure(search|dataframe) used to be document(search|dataframe) akittredge August 2021
from .disclosures import disclosure_search as document_search
from .disclosures import disclosure_dataframe as document_dataframe

from .companies import tickers, companies, companies_raw
from .listener import handle_filings
from .filing import filings, Filing, filings_dataframe
from .metrics import available_metrics, available_metrics_dataframe
from .standardized_numeric import (
    standardized_raw,
    standardized,
)

from .raw_numeric_XBRL import raw_XBRL, raw_xbrl_raw

from .raw_numeric_non_XBRL import non_XBRL_numeric_raw, non_XBRL_numeric

from .dimensional import dimensional_raw, dimensional

from .business_combinations import (
    business_combinations_raw,
    business_combinations,
    legacy_report as business_combination_legacy_report,
)

from .press_release import press_release_raw, press_release_data

from .face_statements import face_statement


def turn_on_logging(level=logging.DEBUG, timezone="US/Eastern") -> logging.Logger:
    """
    Turn on verbose logging

    """
    import pytz

    logging.Formatter.converter = lambda *args: datetime.now(
        tz=pytz.timezone(timezone)
    ).timetuple()
    logging.basicConfig(
        format="%(asctime)s.%(msecs)03d %(levelname)-8s %(message)s", datefmt="%H:%M:%S"
    )
    cb_logger = logging.getLogger("calcbench")
    cb_logger.setLevel(level)

    return cb_logger