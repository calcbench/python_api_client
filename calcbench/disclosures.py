import dataclasses
from dataclasses import dataclass
from datetime import date, datetime
from typing import Generator, Optional, Sequence

try:
    from typing import Literal
except ImportError:
    from typing_extensions import Literal

try:
    from bs4 import BeautifulSoup
except ImportError:
    pass


from calcbench.api_client import (
    CompanyIdentifiers,
    Period,
    PeriodArgument,
    PeriodType,
    _json_GET,
    _json_POST,
    logger,
)

try:
    import pandas as pd
except ImportError:
    "Can't find pandas, won't be able to use the functions that return DataFrames."
    pass

try:
    import tqdm
except ImportError:
    pass


def document_dataframe(
    company_identifiers: CompanyIdentifiers = [],
    disclosure_names: Sequence[str] = [],
    all_history: bool = False,
    year: int = None,
    period: PeriodArgument = None,
    progress_bar: "tqdm.std.tqdm" = None,
    period_type: PeriodType = None,
    identifier_key: Literal["ticker", "CIK"] = "ticker",
    block_tag_names: Sequence[str] = [],
    use_fiscal_period=False,
) -> "pd.DataFrame":
    """Disclosures/Footnotes in a DataFrame

    :param company_identifiers: list of tickers or CIK codes
    :param disclosure_names: The sections to retrieve, see the full list @ https://www.calcbench.com/disclosure_list.  You cannot request XBRL and non-XBRL sections in the same request.  eg.  ['Management's Discussion And Analysis', 'Risk Factors']
    :param all_history: Search all time periods
    :param year: The year to search
    :param period: period of data to get.  0 for annual data, 1, 2, 3, 4 for quarterly data.
    :param period_type: "quarterly" or "annual", only applicable when other period data not supplied.  Use "annual" to only search end-of-year documents.
    :param progress_bar: Pass a tqdm progress bar to keep an eye on things.
    :param identifier_key: "ticker" or "CIK", how to index the returned DataFrame.
    :return: A DataFrame indexed by document name -> company identifier.

    Usage::

      >>> data = calcbench.document_dataframe(company_identifiers=["msft", "goog"], all_history=True, disclosure_names=["Management's Discussion And Analysis", "Risk Factors"])
      >>> data = data.fillna(False)
      >>> word_counts = data.applymap(lambda document: document and len(document.get_contents_text().split()))

    """
    if block_tag_names:
        docs = []
        for block_tag_name in block_tag_names:
            docs.extend(
                document_search(
                    company_identifiers=company_identifiers,
                    block_tag_name=block_tag_name,
                    all_history=all_history,
                    use_fiscal_period=use_fiscal_period,
                    progress_bar=progress_bar,
                    year=year,
                    period=period,
                    period_type=period_type,
                )
            )
    else:
        docs = list(
            document_search(
                company_identifiers=company_identifiers,
                disclosure_names=disclosure_names,
                all_history=all_history,
                use_fiscal_period=True,
                progress_bar=progress_bar,
                year=year,
                period=period,
                period_type=period_type,
            )
        )
    period_map = {"1Q": 1, "2Q": 2, "3Q": 3, "Y": 4}
    for doc in docs:
        period_year = doc["fiscal_year" if use_fiscal_period else "calendar_year"]
        if period in ("Y", 0) or period_type == "annual":
            p = pd.Period(year=period_year, freq="a")
        else:
            try:
                quarter = period_map[
                    doc["fiscal_period" if use_fiscal_period else "calendar_period"]
                ]
            except KeyError:
                # This happens for non-XBRL companies
                logger.info("Strange period for {ticker}".format(**doc))
                p = None
            else:
                p = pd.Period(year=period_year, quarter=quarter, freq="q")
        doc["period"] = p
        doc[identifier_key] = (doc[identifier_key] or "").upper()
        doc["value"] = doc
    data = pd.DataFrame(docs)
    data = data.set_index(keys=[identifier_key, "disclosure_type_name", "period"])
    data = data.loc[~data.index.duplicated()]  # There can be duplicates
    data = data.unstack("disclosure_type_name")["value"]
    data = data.unstack(identifier_key)
    return data


@dataclass
class DocumentSearchResults(dict):
    """
    Represents a disclosure.
    """

    fact_id: int
    entity_name: str
    accession_id: int
    footnote_type: str
    SEC_URL: str
    sec_filing_id: int
    blob_id: str
    fiscal_year: int
    fiscal_period: str
    calendar_year: int
    calendar_period: str
    filing_date: str
    received_date: str
    document_type: str
    guide_link: str
    page_url: str
    entity_id: int
    id_detail: bool
    local_name: str
    CIK: str
    sec_accession_number: str
    network_id: int
    ticker: str
    filing_type: int
    description: str
    disclosure_type_name: str
    period_end_date: str
    footnote_type_title: str

    def __init__(self, **kwargs):
        names = set([f.name for f in dataclasses.fields(self)])
        for k, v in kwargs.items():
            if k in names:
                setattr(self, k, v)
            self[k] = v

    def get_contents(self) -> str:
        """
        Content of the document, with the filers HTML
        """
        if self.get("network_id"):
            return _document_contents_by_network_id(self.network_id)
        else:
            return document_contents(blob_id=self.blob_id, SEC_ID=self["sec_filing_id"])

    def get_contents_text(self) -> str:
        """Contents of the HTML of the document"""
        return "".join(BeautifulSoup(self.get_contents(), "html.parser").strings)

    @property
    def date_reported(self) -> Optional[datetime]:
        """Time (EST) the document was available from Calcbench"""
        return self.get("date_reported") and _try_parse_timestamp(self["date_reported"])


def document_search(
    company_identifiers: CompanyIdentifiers = None,
    full_text_search_term: str = None,
    year: int = None,
    period: PeriodArgument = Period.Annual,
    period_type: Optional[PeriodType] = None,
    document_type: str = None,
    block_tag_name: str = None,
    entire_universe: bool = False,
    use_fiscal_period: bool = False,
    document_name: bool = None,
    all_history: bool = False,
    updated_from: date = None,
    batch_size: int = 100,
    sub_divide: bool = False,
    all_documents: bool = False,
    disclosure_names: Sequence[str] = [],
    progress_bar: "tqdm.std.tqdm" = None,
    accession_id: int = None,
) -> Generator[DocumentSearchResults, None, None]:
    """
    Footnotes and other text

    Search for footnotes and other sections of 10-K, see https://www.calcbench.com/footnote.

    :param company_identifiers: list of tickers or CIK codes
    :param year: Year to get data for
    :param period: period of data to get.  0 for annual data, 1, 2, 3, 4 for quarterly data.
    :param use_fiscal_period: interpret the passed period as a fiscal period, as opposed to calendar period
    :param period_type: only applicable when other period data not supplied.  Use "annual" to only search end-of-year documents.
    :param disclosure_names:  The sections to retrieve, see the full list @ https://www.calcbench.com/disclosure_list.  You cannot request XBRL and non-XBRL sections in the same request.  eg.  ['Management's Discussion And Analysis', 'Risk Factors']
    :param all_history: Search all time periods
    :param updated_from: include filings from this date and after.
    :param sub_divide: return the document split into sections based on headers.
    :param all_documents: all of the documents for a single company/period.
    :param entire_universe: Search all companies
    :param progress_bar: Pass a tqdm progress bar to keep an eye on things.
    :return: A iterator of DocumentSearchResults

    Usage::

       >>> import tqdm
       >>> sp500 = calcbench.tickers(index='SP500')
       >>> with tqdm.tqdm() as progress_bar:
       >>>     risk_factors = list(calcbench.document_search(company_identifiers=sp500, disclosure_names=['Risk Factors'], all_history=True, progress_bar=progress_bar))

    """
    if not any(
        [
            full_text_search_term,
            document_type,
            block_tag_name,
            document_name,
            all_documents,
            disclosure_names,
        ]
    ):
        raise (ValueError("Need to supply at least one search parameter."))
    if not (company_identifiers or entire_universe or accession_id):
        raise (
            ValueError(
                "Need to supply company_identifiers or entire_universe=True or accession_id"
            )
        )
    if not (all_history or updated_from or accession_id):
        if not year:
            raise ValueError("Need to specify year or all all_history")
        period_type = "annual" if period in (0, "Y", "y") else "quarterly"
    payload = {
        "companiesParameters": {"entireUniverse": entire_universe},
        "periodParameters": {
            "year": year,
            "period": period,
            "periodType": period_type,
            "useFiscalPeriod": use_fiscal_period,
            "allHistory": all_history,
            "updatedFrom": updated_from and updated_from.isoformat(),
            "accessionID": accession_id,
        },
        "pageParameters": {
            "fullTextQuery": full_text_search_term,
            "footnoteType": document_type,
            "footnoteTag": block_tag_name,
            "disclosureName": document_name,
            "limit": batch_size,
            "subDivide": sub_divide,
            "allFootnotes": all_documents,
            "disclosureNames": disclosure_names,
        },
    }
    if company_identifiers:
        chunk_size = 30
        for i in range(0, len(company_identifiers), chunk_size):
            payload["companiesParameters"]["companyIdentifiers"] = company_identifiers[
                i : i + chunk_size
            ]
            for r in _document_search_results(payload, progress_bar=progress_bar):
                yield r
    else:
        for r in _document_search_results(payload, progress_bar=progress_bar):
            yield r


def _document_search_results(payload, progress_bar=None):
    results = {"moreResults": True}
    while results["moreResults"]:
        results = _json_POST("footnoteSearch", payload)
        disclosures = results["footnotes"]
        if progress_bar is not None:
            progress_bar.update(len(disclosures))
        for result in disclosures:
            yield DocumentSearchResults(**result)
        payload["pageParameters"]["startOffset"] = results["nextGroupStartOffset"]
    payload["pageParameters"]["startOffset"] = None


def _try_parse_timestamp(timestamp):
    """
    We did not always have milliseconds
    """
    try:
        timestamp = timestamp[:26]  # .net's milliseconds are too long
        return datetime.strptime(timestamp, "%Y-%m-%dT%H:%M:%S.%f")
    except ValueError:
        return datetime.strptime(timestamp, "%Y-%m-%dT%H:%M:%S")


def document_contents(blob_id, SEC_ID, SEC_URL=None) -> str:
    payload = {"blobid": blob_id, "secid": SEC_ID, "url": SEC_URL}
    json = _json_GET("query/disclosureBySECLink", payload)
    return json["blobs"][0]


def _document_contents_by_network_id(network_id) -> str:
    payload = {"nid": network_id}
    json = _json_GET("query/disclosureByNetworkIDOBJ", payload)
    blobs = json["blobs"]
    return blobs[0] if len(blobs) else ""
