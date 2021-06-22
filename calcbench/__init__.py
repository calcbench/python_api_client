"""
Client for the Calcbench API

To turn on verbose logging
import logging
import sys
logger = logging.getLogger()
logging.getLogger('calcbench').setLevel(logging.DEBUG)
logger.addHandler(logging.StreamHandler(stream=sys.stdout))
"""
__version__ = "4.1.1"
from .api_client import (
    CompanyIdentifierScheme,
    as_reported_raw,
    business_combinations,
    company_disclosures,
    disclosure_text,
    document_types,
    enable_backoff,
    face_statement,
    html_diff,
    press_release_raw,
    set_credentials,
    set_proxies,
    tag_contents,
)

from .disclosures import document_search, document_dataframe
from .companies import tickers, companies, companies_raw
from .listener import handle_filings
from .filing import filings, Filing
from .metrics import available_metrics, available_metrics_dataframe
from .standardized_numeric import (
    normalized_data,
    normalized_dataframe,
    normalized_raw,
    point_in_time,
    standardized_data,
    standardized_raw,
)

from .raw_numeric_XBRL import raw_XBRL, raw_xbrl_raw

from .raw_numeric_non_XBRL import non_XBRL_numeric_raw, non_XBRL_numeric

from .dimensional import (
    dimensional_raw,
    business_combinations,
    business_combinations_raw,
)
