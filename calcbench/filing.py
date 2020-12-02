from dataclasses import dataclass
from datetime import date, datetime
from enum import Enum
from typing import Iterable, Optional
import dataclasses

from calcbench.api_client import (
    CompanyIdentifiers,
    Period,
    _json_POST,
    _try_parse_timestamp,
)


class FilingType(str, Enum):
    BusinessWirePR_filedAfterAn8K = "BusinessWirePR_filedAfterAn8K"
    BusinessWirePR_replaced = "BusinessWirePR_replaced"
    proxy = "proxy"
    annualQuarterlyReport = "annualQuarterlyReport"
    eightk_earningsPressRelease = "eightk_earningsPressRelease"
    eightk_guidanceUpdate = "eightk_guidanceUpdate"
    eightk_conferenceCallTranscript = "eightk_conferenceCallTranscript"
    eightk_presentationSlides = "eightk_presentationSlides"
    eightk_monthlyOperatingMetrics = "eightk_monthlyOperatingMetrics"
    eightk_earningsPressRelease_preliminary = "eightk_earningsPressRelease_preliminary"
    eightk_earningsPressRelease_correction = "eightk_earningsPressRelease_correction"
    eightk_other = "eightk_other"
    commentLetter = "commentLetter"
    commentLetterResponse = "commentLetterResponse"
    form_3 = "form_3"
    form_4 = "form_4"
    form_5 = "form_5"
    eightk_nonfinancial = "eightk_nonfinancial"
    NT10KorQ = "NT10KorQ"
    S = "S"
    Four24B = "Four24B"
    institutionalOwnsership_13F = "institutionalOwnsership_13F"


@dataclass
class Filing(dict):
    is_xbrl: bool
    is_wire: bool
    calcbench_id: int
    sec_accession_id: str
    sec_html_url: str
    document_type: str
    filing_type: FilingType
    filing_sub_type: str
    filing_date: datetime
    fiscal_period: Period
    fiscal_year: int
    calcbench_accepted: datetime
    calcbench_finished_load: datetime
    entity_id: int
    ticker: str
    entity_name: str
    CIK: str
    period_index: int
    associated_proxy_SEC_URL: str
    associated_earnings_press_release_SEC_URL: str
    period_end_date: datetime
    percentage_revenue_change: Optional[float]
    this_period_revenue: Optional[float]
    link1: str
    link2: str
    link3: str
    calendar_year: int
    calendar_period: Period
    standardized_XBRL: bool

    def __init__(self, **kwargs):
        names = set([f.name for f in dataclasses.fields(self)])
        for name in names:
            setattr(self, name, None)
        for k, v in kwargs.items():
            if k in (
                "calcbench_finished_load",
                "calcbench_accepted",
                "filing_date",
                "period_end_date",
            ):
                v = _try_parse_timestamp(v)
            if k in names:
                setattr(self, k, v)
            self[k] = v


def filings(
    company_identifiers: CompanyIdentifiers = [],
    entire_universe: bool = False,
    include_non_xbrl: bool = True,
    received_date: date = None,
    start_date: date = None,
    end_date: date = None,
    filing_types=[],
) -> Iterable[Filing]:
    """SEC filings

    https://www.calcbench.com/filings

    :param company_identifiers: list of tickers or CIK codes
    :param received_date: get all filings received on this date
    :param entire_universe: filings for all companies
    :param include_non_xbrl: include filings that do not have XBRL, 8-Ks, earnings releases etc.
    :param received_data: only filings published on this date
    :param start_date: filings received on or after this date
    :param end_date: filings received on or before theis date

    Usage::
        >>> from datetime import date
        >>> calcbench.filings(received_date=date.today(), entire_universe=True)

    """

    filings = _json_POST(
        "filingsV2",
        {
            "companiesParameters": {
                "companyIdentifiers": list(company_identifiers),
                "entireUniverse": entire_universe,
            },
            "pageParameters": {
                "includeNonXBRL": include_non_xbrl,
                "filingTypes": filing_types,
            },
            "periodParameters": {
                "updateDate": received_date and received_date.isoformat(),
                "dateRange": start_date
                and end_date
                and {
                    "startDate": start_date.isoformat(),
                    "endDate": end_date.isoformat(),
                },
                "asOriginallyReported": False,
            },
        },
    )
    for filing in filings:
        yield Filing(**filing)