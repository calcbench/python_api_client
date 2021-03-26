from typing import Optional, Sequence

try:
    from typing import Literal
except ImportError:
    from typing_extensions import Literal

from calcbench.api_client import CompanyIdentifiers, _json_POST

try:
    import pandas as pd
except ImportError:
    pass

Index = Literal["DJIA", "SP500"]


def tickers(
    SIC_codes: Sequence[int] = [],
    index: Optional[Index] = None,
    company_identifiers: CompanyIdentifiers = [],
    entire_universe: bool = False,
    NAICS_codes: Sequence[int] = [],
) -> "list[str]":
    """Get tickers

    :param SIC_codes: Sequence of SIC (Standard Industrial Classification) codes. eg. [1200, 1300]
    :param index: 'DJIA' or 'SP500'
    :param company_identifiers: tickers
    :param entire_universe: all of the companies in the Calcbench database
    :param NAICS_codes: Sequence of NAICS codes
    :return: list of tickers

    Usage::

        >>> calcbench.tickers(SIC_codes=[1100])

    """
    companies = _companies(
        SIC_codes,
        index,
        list(company_identifiers),
        entire_universe,
        NAICS_codes=NAICS_codes,
    )
    tickers = [co["ticker"] for co in companies]
    return tickers


def companies(
    SIC_codes: Sequence[int] = [],
    index: Optional[Index] = None,
    company_identifiers: CompanyIdentifiers = [],
    entire_universe: bool = False,
    include_most_recent_filing_dates: bool = False,
    NAICS_codes: Sequence[int] = [],
) -> "pd.DataFrame":
    """Return a DataFrame with company details

    :param SIC_codes: Sequence of SIC (Standard Industrial Classification) codes. eg. [1200, 1300]
    :param index: 'DJIA' or 'SP500'
    :param company_identifiers: tickers
    :param entire_universe: all of the companies in the Calcbench database
    :param NAICS_codes: Sequence of NAICS codes
    :return: Dataframe with data about companies

    Usage::

        >>> calcbench.tickers(company_identifiers=["msft", "orcl"])

    """
    companies = _companies(
        SIC_codes,
        index,
        list(company_identifiers),
        entire_universe,
        include_most_recent_filing_dates,
        NAICS_codes=NAICS_codes,
    )

    companies = pd.DataFrame(companies)
    if not companies.empty:
        for column in [
            "first_filing",
            "most_recent_filing",
            "most_recent_full_year_end",
        ]:
            companies[column] = pd.to_datetime(companies[column], errors="coerce")
    return companies


def _companies(
    SIC_code,
    index: Optional[Index],
    company_identifiers,
    entire_universe=False,
    include_most_recent_filing_dates=False,
    NAICS_codes=None,
):
    if not (SIC_code or index or entire_universe or company_identifiers, NAICS_codes):
        raise ValueError(
            "Must supply SIC_code, NAICS_codes, index or company_identifiers or entire_universe."
        )
    elif entire_universe and any([SIC_code, index, company_identifiers]):
        raise ValueError("entire_universe with other parameters does not make sense.")
    payload = {}

    if index:
        if index not in ("SP500", "DJIA"):
            raise ValueError("index must be either 'SP500' or 'DJIA'")
        payload["index"] = index
    elif SIC_code:
        payload["SICCodes"] = SIC_code
    elif NAICS_codes:
        payload["NAICSCodes"] = NAICS_codes
    elif company_identifiers:
        payload["companyIdentifiers"] = company_identifiers
    else:
        payload["universe"] = True
    payload["includeMostRecentFilingExtras"] = include_most_recent_filing_dates
    return _json_POST("companies", payload)


def companies_raw(
    SIC_codes=[], index=None, company_identifiers=[], entire_universe=False
):
    return _companies(SIC_codes, index, company_identifiers, entire_universe)
