from datetime import datetime
from enum import Enum
from typing import TYPE_CHECKING, Any, Callable, Optional, Sequence, Union
from typing_extensions import Annotated

from pydantic import BaseModel, BeforeValidator, ValidationError, WrapValidator
from calcbench.api_client import _json_GET
from calcbench.api_query_params import Period


if TYPE_CHECKING:
    # https://github.com/microsoft/pyright/issues/1358
    from bs4 import BeautifulSoup
else:
    try:
        from bs4 import BeautifulSoup
    except ImportError:
        pass


class FootnoteTypeTitle(str, Enum):
    """
    This should probably not exist.  I think there are an infinite number of titles.
    """

    EigthKsByItemType = "8-Ks By Item Type"
    AccountingPolicies = "Accounting Policies"
    AdditionalSections = "Additional 10-K and 10-Q Sections"
    BusinessCombinations = "Business Combinations"
    CashAndEquivalents = "Cash And Cash Equivalents"
    CommitmentsAndContigencies = "Commitment And Contingencies"
    Compensation = "Compensation Related Costs Postemployment Benefits"
    Debt = "Debt"
    Derivatives = "Derivative Instruments And Hedging Activities"
    EarningPerShare = "Earnings Per Share"
    Equity = "Equity"
    Goodwill = "Goodwill & Intangible Assets"
    IncomeTax = "Income Tax"
    InterimReporting = "Interim Reporting"
    Inventory = "Inventory & PPE"
    Leases = "Leases"
    Other = "Other"
    OtherExpenses = "Other Expenses"
    RelatedDocuments = "Related Documents (Earnings, Proxys & Letters)"
    RevenueFromContractWithCustomer = "Revenuefrom Contract With Customer"
    Segment = "Segment"
    PolicyTextBlock = "Policy Text Block"
    TextBlock = "Text Block"
    DeferredRevenue = "Deferred Revenue"
    ExitOrDisposalCostObligations = "Exit Or Disposal Cost Obligations"
    FinancialInstrumentsAtFairValue = "Financial Instruments At Fair Value"
    InvestmentsDebtAndEquitySecurities = "Investments Debt And Equity Securities"


def footnote_type_title_validator(v: Any, handler: Callable[[Any], Any]) -> Any:
    """
    The enum does not have all of the possible Type Titles.
    """
    try:
        return handler(v)
    except ValidationError:
        return None


def _build_period(p: str) -> Union[Period, str]:
    return PERIOD_MAP.get(p, p)


class DisclosureContent(BaseModel, extra="allow"):
    """
    Contents of an individual disclosure
    Corresponds to XBRLDisclosure on the server
    """

    blobs: Sequence[str]
    """
    There will be more than one blob when multiple blocks map to the same network.
    """
    entity_id: int

    entity_name: str
    document_type: Optional[str]
    """
    Not set in single company mode
    """

    sec_html_url: str
    sec_accession_number: Optional[str] = None
    accession_id: int
    label: str
    fact_id: int
    disclosure_type: int
    """
    ArcRole
    """
    is_detail: bool
    fiscal_period: Annotated[Optional[Period], BeforeValidator(_build_period)]
    """
    Not set in single company mode
    """
    fiscal_year: Optional[int]
    """
    Not set in single company mode
    """
    last_in_group: bool
    networkID: int
    ticker: Optional[str] = None
    table_list: Optional[list] = None
    local_name: Optional[str] = None
    CIK: Optional[str] = None

    @property
    def contents(self) -> str:
        return "</br>".join(self.blobs)


class DisclosureSearchResults(BaseModel, extra="allow"):
    """
    An individual disclosure.

    A list of these is returned by the disclosure_search function.
    """

    fact_id: Optional[int] = None
    entity_name: Optional[str]
    """
    Not set in single company mode
    """
    accession_id: Optional[int]
    """
    Not set in single company mode
    """
    footnote_type: Optional[int]
    SEC_URL: Optional[str]
    """
    Not set in single company mode
    """
    sec_filing_id: Optional[int]
    blob_id: Optional[str]
    fiscal_year: Optional[int]
    """
    Not set in single company mode
    """
    fiscal_period: Annotated[Optional[Period], BeforeValidator(_build_period)]
    """
    Not set in single company mode
    """
    calendar_year: Optional[int]
    """
    Not set in single company mode
    """
    calendar_period: Annotated[Optional[Period], BeforeValidator(_build_period)]
    filing_date: str
    received_date: str
    document_type: Optional[str]
    """
    Not set in single company mode
    """

    guide_link: Optional[str]
    page_url: Optional[str]
    entity_id: int
    id_detail: bool
    local_name: Optional[str]
    CIK: Optional[str]
    """
    Not set in single company mode
    """
    sec_accession_number: Optional[str]
    network_id: Optional[int]
    ticker: str

    filing_type: int
    """
    Filing type from the filing type enum
    """

    description: str
    """
    The name passed to the API, not set for 8-Ks, assigned by Calcbench.  We try to assign each disclosure to a category.
    """

    disclosure_type_name: Optional[str]
    """
    Pass this to the API.  For XBRL tagged notes this is the DisclosureCategory as defined by the FASB.
    """
    period_end_date: Optional[str]
    """
    Not set in single company mode
    """
    footnote_type_title: Annotated[
        Union[FootnoteTypeTitle, None], WrapValidator(footnote_type_title_validator)
    ]
    """
    Not set in single company mode
    """

    content: Optional[DisclosureContent] = None
    """
    Not always set.  Set for MD&A sections for example.
    """

    date_reported: Optional[datetime]
    """Time (EST) the document was available from Calcbench"""

    name: str
    """
    Section or disclosure name, block tag name.  Use this instead of description or local name.
    """

    def get_contents(self) -> str:
        """
        Content of the document, with the filers HTML
        """
        return self.get_disclosure().contents

    def get_contents_text(self) -> str:
        """Contents of the HTML of the document"""
        return "".join(BeautifulSoup(self.get_contents(), "html.parser").strings)

    def get_disclosure(self) -> DisclosureContent:
        """
        Content of the document, with the filer's HTML
        """
        if self.content:
            return self.content
        elif self.network_id:
            return _document_contents_by_network_id(self.network_id)
        elif self.local_name:
            return _document_by_block_tag_name(
                accession_id=self.accession_id, block_tag_name=self.local_name
            )
        else:
            return _document_contents(blob_id=self.blob_id, SEC_ID=self.sec_filing_id)

    def __str__(self):
        return f'DisclosureSearchResults(ticker="{self.ticker}", name="{self.name}", fiscal_year={self.fiscal_year}, fiscal_period={self.fiscal_period})'


def _document_contents(blob_id, SEC_ID, SEC_URL=None) -> DisclosureContent:
    payload = {"blobid": blob_id, "secid": SEC_ID, "url": SEC_URL}
    json = _json_GET("query/disclosureBySECLink", payload)
    return DisclosureContent(**json)


def _document_contents_by_network_id(network_id) -> DisclosureContent:
    payload = {"nid": network_id}
    json = _json_GET("query/disclosureByNetworkIDOBJ", payload)
    return DisclosureContent(**json)


def _document_by_block_tag_name(
    accession_id: int, block_tag_name: str
) -> DisclosureContent:
    payload = {"accession_ids": accession_id, "block_tag_name": block_tag_name}
    json = _json_GET("query/disclosuresByTag", payload)
    return DisclosureContent(**json[0])


PERIOD_MAP = {
    "1Q": Period.Q1,
    "2Q": Period.Q2,
    "3Q": Period.Q3,
    "Y": Period.Annual,
    "1": Period.Q1,
    "2": Period.Q2,
    "3": Period.Q3,
    "4": Period.Q4,
}


class DisclosureAPIPageParameters(BaseModel):
    """
    Parameters specific the disclosure API end-point

    :meta private:
    """

    fullTextQuery: Optional[str] = None
    footnoteType: Optional[str] = None
    footnoteTag: Optional[str] = None
    disclosureName: Optional[str] = None
    limit: Optional[int] = None
    subDivide: Optional[bool] = False
    allFootnotes: Optional[bool] = None
    disclosureNames: Optional[Sequence[str]] = None
    AllTextBlocks: Optional[bool] = False
    startOffset: Optional[int] = None
