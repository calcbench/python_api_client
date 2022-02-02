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
    """A filing with the SEC or a wire press-release"""

    is_xbrl: bool
    is_wire: bool
    calcbench_id: int
    sec_accession_id: str
    sec_html_url: str

    document_type: str
    """
    The label assigned to the filing by the SEC

    /FI,/MD,10-12B ,10-12B [Cover],10-12B/A [Amend] ,10-12B/A [Amend] [Cover],10-12G ,10-12G [Cover],10-12G/A,10-12G/A [Amend] ,10-12G/A [Amend] [Cover],10-D ,10-D/A [Amend] ,10-K,10-K [Cover],10-K [Paper],10-K/A,10-K/A [Amend] ,10-K/A [Amend] [Cover],10-KT ,10-KT/A,10-KT/A [Amend] ,10-KT/A [Amend] [Cover],10-Q,10-Q [Cover],10-Q [Paper],10-Q/A,10-Q/A [Amend] ,10-Q/A [Amend] [Cover],10-QT ,10-QT/A,10-QT/A [Amend] ,13F-HR,20-F,20-F/A,20FR12G,3,4,4/A,40-F,40-F/A,424B3,424B4,424B5,425,485APOS,485BPOS,485BXT,497,5,6-K,6-K/A,8-K,8-K [Cover],8-K/A,8-K/A [Amend] ,8-K/A [Amend] [Cover],8-K12B,8-K12B/A,8-K12B/A [Amend] ,8-K12G3,8-K12G3/A,8-K12G3/A [Amend] ,8-K15D5 ,ANNLRPT,Annual,ARS,CORRESP,DEF 14A,DEF 14C,DEFA14A,DEFC14A,DEFM14A,DEFM14C,DEFN14A,DEFR14A,DEFR14C,ESMA,F-1,F-1/A,F-3,F-3/A,F-3ASR,F-4,F-4/A,FOCUSN,HalfYear,MSD/A,MSDW,N-1A/A,NT 10-D ,NT 10-K,NT 10-K/A,NT 10-K/A [Amend] ,NT 10-Q,NT 10-Q/A,NT 10-Q/A [Amend] ,NT 11-K ,NT 20-F,NT 20-F/A,NT 20-F/A [Amend] ,NT N-CEN,NT N-MFP ,NT NPORT-EX,NT NPORT-P,NT-NCSR ,NT-NSAR ,NTN 10K ,NTN 10Q ,POS AM,POS EX,POSASR,PRE 14A,S-1,S-1/A,S-11,S-11/A,S-11MEF,S-1MEF,S-3,S-3/A,S-3ASR,S-4,S-4/A,SDR/A,SP 15D2,UPLOAD,WIREPR,X-17A-5,X-17A-5/A'
    """

    filing_type: FilingType
    """
    Standardized FilingType as assigned by Calcbench
    """

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

    https://www.calcbench.com/filings.

    Note that records are returned from the API that do not appear on the filings page.  On the filings page we only show records for which the is a record for the filer in our entities table.

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

    df["filing_date"] = pd.to_datetime(
        df["filing_date"], errors="coerce"
    )  # filing dates can be strange
    for column in ["calendar_period", "fiscal_period"]:
        df[column] = df[column].astype(period_number)

    return df
