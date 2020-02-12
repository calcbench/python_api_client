"""
Client for the Calcbench API

To turn on verbose logging
import logging
logging.getLogger('calcbench.api_client').setLevel(logging.INFO)
"""
__version__ = "2.2.0"
from .api_client import (
    normalized_data,
    normalized_dataframe,
    standardized_data,
    tickers,
    set_credentials,
    set_proxies,
    companies,
    normalized_raw,
    as_reported_raw,
    dimensional_raw,
    companies_raw,
    company_disclosures,
    disclosure_text,
    available_metrics,
    document_search,
    filings,
    mapped_raw,
    point_in_time,
    document_contents,
    tag_contents,
    business_combinations,
    document_types,
    html_diff,
    press_release_raw,
    document_dataframe,
    enable_backoff,
    raw_xbrl_raw,
    raw_xbrl,
    DocumentSearchResults,
)

from .listener import handle_filings

