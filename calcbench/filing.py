from datetime import date
from typing import List, Optional, Sequence

from calcbench.models.filing import Filing
from calcbench.models.filing_type import FilingType


try:
    import pandas as pd
    from calcbench.standardized_numeric import period_number
except ImportError:
    "Can't find pandas, won't be able to use the functions that return DataFrames."
    pass

from calcbench.api_client import (
    _json_POST,
)
from calcbench.api_query_params import CompanyIdentifiers


def filings(
    company_identifiers: CompanyIdentifiers = [],
    entire_universe: bool = False,
    include_non_xbrl: bool = True,
    received_date: Optional[date] = None,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    include_press_releases_and_proxies: bool = True,
    filing_types: Sequence[FilingType] = [],
) -> List[Filing]:
    """SEC filings

    https://www.calcbench.com/filings.

    Note that records are returned from the API that do not appear on the filings page.  On the filings page we only show records for which there is a record for the filer in our entities table.

    :param company_identifiers: list of tickers or CIK codes
    :param received_date: get all filings received on this date
    :param entire_universe: filings for all companies
    :param include_non_xbrl: include filings that do not have XBRL, 8-Ks, earnings releases etc.
    :param received_date: only filings published by Calcbench on this date
    :param start_date: filings published by Calcbench on or after this date
    :param end_date: filings published by Calcbench on or before theis date
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
    return [Filing(**f) for f in filings]


def filings_dataframe(
    company_identifiers: CompanyIdentifiers = [],
    entire_universe: bool = False,
    include_non_xbrl: bool = True,
    received_date: Optional[date] = None,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    include_press_releases_and_proxies: bool = True,
    filing_types: Sequence[FilingType] = [],
) -> "pd.DataFrame":
    """SEC filings in a dataframe

    https://www.calcbench.com/filings

    :param company_identifiers: list of tickers or CIK codes
    :param received_date: get all filings received on this date
    :param entire_universe: filings for all companies
    :param include_non_xbrl: include filings that do not have XBRL, 8-Ks, earnings releases etc.
    :param received_date: only filings published by Calcbench on this date
    :param start_date: filings published by Calcbench on or after this date
    :param end_date: filings published by Calcbench on or before theis date
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
    df = pd.DataFrame([filing.dict() for filing in f])
    for column in [
        "document_type",
        "filing_type",
    ]:
        df[column] = pd.Categorical(df[column])

    df["filing_date"] = pd.to_datetime(
        df["filing_date"], errors="coerce"
    )  # filing dates can be strange
    for column in ["calendar_period", "fiscal_period"]:
        df[column] = df[column].astype(period_number)
    df["fiscal_year"] = df["fiscal_year"].astype(pd.Int32Dtype())
    df = df.set_index("filing_id")
    df = df.sort_index(ascending=False)
    return df[
        [
            "ticker",
            "entity_name",
            "CIK",
            "is_xbrl",
            "is_wire",
            "has_standardized_data",
            "document_type",
            "filing_type",
            "filing_date",
            "calcbench_accepted",
            "calcbench_finished_load",
            "fiscal_year",
            "fiscal_period",
            "sec_html_url",
        ]
    ]
