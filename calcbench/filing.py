from dataclasses import dataclass
from datetime import date, datetime
from enum import Enum
from typing import Iterable, Optional, Sequence
import dataclasses


try:
    import pandas as pd
    from calcbench.standardized_numeric import period_number
except ImportError:
    "Can't find pandas, won't be able to use the functions that return DataFrames."
    pass

from calcbench.api_client import (
    _json_POST,
    _try_parse_timestamp,
)
from calcbench.api_query_params import CompanyIdentifiers, Period


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

    @property
    def accession_id(self) -> int:
        """same as calcbench_id, calcbench_id should have been accession_id"""
        return self.calcbench_id

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
    include_press_releases_and_proxies: bool = True,
    filing_types: Sequence[FilingType] = [],
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
    :param filing_type: types of filings to include

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
                "includePressReleasesAndProxies": include_press_releases_and_proxies,
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


def filings_dataframe(
    company_identifiers: CompanyIdentifiers = [],
    entire_universe: bool = False,
    include_non_xbrl: bool = True,
    received_date: date = None,
    start_date: date = None,
    end_date: date = None,
    include_press_releases_and_proxies: bool = True,
    filing_types: Sequence[FilingType] = [],
) -> "pd.DataFrame":
    """SEC filings in a dataframe

    https://www.calcbench.com/filings

    :param company_identifiers: list of tickers or CIK codes
    :param received_date: get all filings received on this date
    :param entire_universe: filings for all companies
    :param include_non_xbrl: include filings that do not have XBRL, 8-Ks, earnings releases etc.
    :param received_data: only filings published on this date
    :param start_date: filings received on or after this date
    :param end_date: filings received on or before theis date
    :param filing_type: types of filings to include

    Usage::
        >>> import calcbench as cb
        >>> from calcbench.filing import FilingType
        >>> cb.filings_dataframe(
        >>>     entire_universe=True,
        >>>     filing_types=[FilingType.commentLetter, FilingType.commentLetterResponse],
        >>> )

    """
    f = filings(
        company_identifiers=company_identifiers,
        entire_universe=entire_universe,
        include_non_xbrl=include_non_xbrl,
        received_date=received_date,
        start_date=start_date,
        end_date=end_date,
        include_press_releases_and_proxies=include_press_releases_and_proxies,
        filing_types=filing_types,
    )
    df = pd.DataFrame(list(f))
    for column in [
        "document_type",
        "filing_type",
        "filing_sub_type",
        "CIK",
        "ticker",
        "entity_name",
    ]:
        df[column] = pd.Categorical(df[column])

    for column in ["calendar_period", "fiscal_period"]:
        df[column] = df[column].astype(period_number)

    return df
