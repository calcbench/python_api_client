from datetime import date
from typing import Generator, Iterable, Optional, Sequence

from calcbench.api_query_params import (
    APIQueryParams,
    CompaniesParameters,
    CompanyIdentifiers,
    Period,
    PeriodArgument,
    PeriodParameters,
    PeriodType,
)
from calcbench.models.disclosure import (
    DisclosureAPIPageParameters,
    DisclosureSearchResults,
)

try:
    from typing import Literal
except ImportError:
    from typing_extensions import Literal


from calcbench.api_client import (
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


def disclosure_search(
    company_identifiers: Optional[CompanyIdentifiers] = None,
    full_text_search_term: Optional[str] = None,
    year: Optional[int] = None,
    period: PeriodArgument = Period.Annual,
    period_type: Optional[PeriodType] = None,
    document_type: Optional[str] = None,
    block_tag_name: Optional[str] = None,
    entire_universe: bool = False,
    use_fiscal_period: bool = False,
    document_name: Optional[str] = None,
    all_history: bool = False,
    updated_from: Optional[date] = None,
    batch_size: int = 100,
    sub_divide: bool = False,
    all_documents: bool = False,
    disclosure_names: Sequence[str] = [],
    progress_bar: Optional["tqdm.std.tqdm"] = None,
    accession_id: Optional[int] = None,
    all_text_blocks: bool = False,
) -> Generator[DisclosureSearchResults, None, None]:
    """
    Footnotes and other text

    Search for footnotes and other sections of 10-K, see https://www.calcbench.com/footnote.

    Formerly know as "document_search"

    :param company_identifiers: list of tickers or CIK codes
    :param year: Year to get data for
    :param full_text_search_term: Use Calcbench's full text index to search documents.  Documents are returned in decreasing order of relevance as defined by TF-IDF.  Use Lucene query syntax, https://lucene.apache.org/core/2_9_4/queryparsersyntax.html.  You can restrict the documents search by setting the `document_type`.  For instance, 'commentLetter', 'EarningsPressReleaseFrom8K', 'eightk_all_types'
    :param period: period of data to get.  0 for annual data, 1, 2, 3, 4 for quarterly data.
    :param use_fiscal_period: interpret the passed period as a fiscal period, as opposed to calendar period
    :param period_type: only applicable when other period data not supplied.  Use "annual" to only search end-of-year disclosures, "quarterly" is all history all periods
    :param document_type: Search a specific document type.
    :param disclosure_names:  The sections to retrieve, see the full list @ https://www.calcbench.com/disclosure_list.  You cannot request XBRL and non-XBRL sections in the same request.  eg.  ['Management's Discussion And Analysis', 'Risk Factors']
    :param all_history: Search all time periods
    :param updated_from: include filings from this date and after.
    :param sub_divide: return the disclosures split into sections based on headers.
    :param all_documents: all of the documents for a single company/period.
    :param entire_universe: Search all companies
    :param progress_bar: Pass a tqdm progress bar to keep an eye on things.
    :param block_tag_name: Level 2 or 3 XBRL tag.  See the list of FASB tags @ https://www.calcbench.com/disclosure_list#blockTags
    :param all_text_blocks: All level 1 and accounting policy text blocks
    :return: A iterator of DisclosureSearchResults

    Usage::

       >>> import tqdm
       >>> sp500 = calcbench.tickers(index="SP500")
       >>> with tqdm.tqdm() as progress_bar:
       >>>     risk_factors = calcbench.disclosure_search(
       >>>          company_identifiers=sp500,
       >>>          disclosure_names=["RiskFactors"],
       >>>          all_history=True,
       >>>          progress_bar=progress_bar
       >>>     )

    """
    if not any(
        [
            full_text_search_term,
            document_type,
            block_tag_name,
            document_name,
            all_documents,
            disclosure_names,
            all_text_blocks,
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
        period_type = (
            PeriodType.Annual if period in (0, "Y", "y") else PeriodType.Quarterly
        )
    if all_history and period_type == PeriodType.Quarterly:
        # The server handles quarterly weird, passing nothing gets you all disclosures which is what you want
        period_type = None

    page_parameters = DisclosureAPIPageParameters(
        **{
            "fullTextQuery": full_text_search_term,
            "footnoteType": document_type,
            "footnoteTag": block_tag_name,
            "disclosureName": document_name,
            "limit": batch_size,
            "subDivide": sub_divide,
            "allFootnotes": all_documents,
            "disclosureNames": disclosure_names,
            "AllTextBlocks": all_text_blocks,
        }
    )
    period_parameters = PeriodParameters(
        **{
            "year": year,
            "period": period,
            "periodType": period_type,
            "useFiscalPeriod": use_fiscal_period,
            "allHistory": all_history,
            "updatedFrom": updated_from and updated_from.isoformat(),
            "accessionID": accession_id,
        }
    )

    if company_identifiers:
        chunk_size = 30
        for i in range(0, len(company_identifiers), chunk_size):
            company_identifier_chunk = company_identifiers[i : i + chunk_size]
            company_parameters = CompaniesParameters(
                companyIdentifiers=company_identifier_chunk, entireUniverse=False
            )
            payload = APIQueryParams(
                companiesParameters=company_parameters,
                periodParameters=period_parameters,
                pageParameters=page_parameters,
            )

            for r in _document_search_results(payload, progress_bar=progress_bar):
                yield r
    else:
        payload = APIQueryParams(
            companiesParameters=CompaniesParameters(
                entireUniverse=True, companyIdentifiers=None
            ),
            periodParameters=period_parameters,
            pageParameters=page_parameters,
        )
        for r in _document_search_results(payload, progress_bar=progress_bar):
            yield r


def disclosure_dataframe(
    company_identifiers: CompanyIdentifiers = [],
    disclosure_names: Sequence[str] = [],
    all_history: bool = False,
    year: Optional[int] = None,
    period: PeriodArgument = None,
    progress_bar: Optional["tqdm.std.tqdm"] = None,
    period_type: Optional[PeriodType] = None,
    identifier_key: Literal["ticker", "CIK"] = "ticker",
    block_tag_names: Sequence[str] = [],
    use_fiscal_period: bool = True,
    entire_universe: bool = False,
    batch_size: int = 100,
) -> "pd.DataFrame":
    """Disclosures/Footnotes in a DataFrame

    formerly know as "document_dataframe"

    :param company_identifiers: list of tickers or CIK codes
    :param disclosure_names: The sections to retrieve, see the full list @ https://www.calcbench.com/disclosure_list.  You cannot request XBRL and non-XBRL sections in the same request.  eg.  ['Management's Discussion And Analysis', 'Risk Factors']
    :param all_history: Search all time periods
    :param year: The year to search
    :param period: period of data to get
    :param period_type: Only applicable when other period data not supplied.  Use "annual" to only search end-of-year documents, "quarterly" is all history all periods
    :param progress_bar: Pass a tqdm progress bar to keep an eye on things.
    :param identifier_key: how to index the returned DataFrame.
    :param use_fiscal_period: Index disclosure by fiscal, as opposed to calendar periods.
    :param entire_universe: Data for all companies
    :return: A DataFrame of DisclosureSearchResults indexed by document name -> company identifier.  An empty frame if no results are found.

    Usage::

      >>> data = calcbench.disclosure_dataframe(company_identifiers=["msft", "goog"],
      >>>                                     all_history=True,
      >>>                                     disclosure_names=["ManagementsDiscussionAndAnalysis", "RiskFactors"],
      >>>                                     period_type="annual")
      >>> word_counts = data.applymap(lambda disclosure: disclosure.get_contents_text().split()
      >>>                             na_action="ignore")

    """
    if block_tag_names:
        docs: Iterable[DisclosureSearchResults] = []
        for block_tag_name in block_tag_names:
            docs.extend(
                disclosure_search(
                    company_identifiers=company_identifiers,
                    block_tag_name=block_tag_name,
                    all_history=all_history,
                    use_fiscal_period=use_fiscal_period,
                    progress_bar=progress_bar,
                    year=year,
                    period=period,
                    period_type=period_type,
                    entire_universe=entire_universe,
                    batch_size=batch_size,
                )
            )
    else:
        docs = disclosure_search(
            company_identifiers=company_identifiers,
            disclosure_names=disclosure_names,
            all_history=all_history,
            use_fiscal_period=use_fiscal_period,
            progress_bar=progress_bar,
            year=year,
            period=period,
            period_type=period_type,
            entire_universe=entire_universe,
            batch_size=batch_size,
        )
    all_docs = []
    for doc in docs:
        period_year = doc.fiscal_year if use_fiscal_period else doc.calendar_year
        if not period_year:
            logger.info(f"Bad year for {doc}")
            continue
        if period in ("Y", 0) or period_type == PeriodType.Annual:
            p = pd.Period(year=period_year, freq="a")  # type: ignore
        else:
            try:
                if period_type == PeriodType.Quarterly:
                    # The server is not handling period type correctly.  Doing it here because it is easier.  akittredge, July 2021.
                    if use_fiscal_period:
                        if doc.fiscal_period == Period.Annual:
                            quarter = Period.Q4
                        else:
                            quarter = doc.fiscal_period
                    else:
                        quarter = doc.calendar_period
                else:
                    raise ValueError("Must pass period_type or period")

            except KeyError:
                # This happens for non-XBRL companies
                logger.info("Strange period for {ticker}".format(doc.ticker))
                p = None
            else:
                p = pd.Period(year=period_year, quarter=quarter, freq="q")  # type: ignore
        doc_dict = doc.model_dump()
        all_docs.append(
            {
                **doc_dict,
                **{
                    "period": p,
                    identifier_key: (doc_dict[identifier_key] or ""),
                    "value": doc,
                },
            },
        )
    if not all_docs:
        # No results, return an empty frame.
        return pd.DataFrame()
    data = pd.DataFrame(all_docs)
    data = data.set_index(keys=[identifier_key, "disclosure_type_name", "period"])  # type: ignore
    data = data.loc[~data.index.duplicated()]  # type:ignore There can be duplicates
    data = data.unstack("disclosure_type_name")["value"]  # type: ignore
    data = data.unstack(identifier_key)
    return data


def _document_search_results(
    payload: APIQueryParams[DisclosureAPIPageParameters],
    progress_bar: Optional["tqdm.std.tqdm"] = None,
):
    """
    Keep making requests until the server returns "moreResults" == false
    """
    results = {"moreResults": True}
    while results["moreResults"]:
        results = _json_POST("footnoteSearch", payload)
        if not results:
            return
        disclosures = results["footnotes"]
        if progress_bar is not None:
            progress_bar.update(len(disclosures))
        for result in disclosures:
            yield DisclosureSearchResults(**result)
        payload.pageParameters.startOffset = results["nextGroupStartOffset"]

    payload.pageParameters.startOffset = None
