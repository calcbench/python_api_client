"""
Client for the Calcbench API

To turn on verbose logging
import logging
logging.getLogger('calcbench.api_client').setLevel(logging.INFO)
"""
__version__ = "3.0.14"
from .api_client import (
    CompanyIdentifierScheme,
    as_reported_raw,
    business_combinations,
    company_disclosures,
    dimensional_raw,
    disclosure_text,
    document_types,
    enable_backoff,
    face_statement,
    html_diff,
    mapped_raw,
    normalized_data,
    normalized_dataframe,
    normalized_raw,
    point_in_time,
    press_release_raw,
    raw_data,
    raw_data_raw,
    raw_xbrl,
    raw_xbrl_raw,
    set_credentials,
    set_proxies,
    standardized_data,
    tag_contents,
)
from .disclosures import document_contents, document_search
from .companies import tickers, companies, companies_raw
from .listener import handle_filings
from .filing import filings, Filing
from .metrics import available_metrics, available_metrics_dataframe
