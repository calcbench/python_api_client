from datetime import datetime
from enum import Enum
from typing import Optional, Sequence, Union
from typing_extensions import Annotated
from bs4 import BeautifulSoup
from pydantic import BaseModel, BeforeValidator
from calcbench.api_client import _json_GET
from calcbench.api_query_params import Period


class FootnoteTypeTitle(str, Enum):
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
    RelatedDocuments = "Related Documents (8-Ks, Proxys & Letters)"
    RevenueFromContractWithCustomer = "Revenuefrom Contract With Customer"
    Segment = "Segment"
    PolicyTextBlock = "Policy Text Block"
    TextBlock = "Text Block"


def _build_period(p: str) -> Union[Period, str]:
    return PERIOD_MAP.get(p, p)


class DisclosureContent(BaseModel, extra="allow"):
    """
    Corresponds to XBRLDisclosure on the server
    """

    blobs: Sequence[str]
    """
    There will be more than one blob when multiple blocks map to the same network.
    """
    entity_id: int

    entity_name: str
    document_type: str

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
    fiscal_year: int
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
    """

    fact_id: Optional[int] = None
    entity_name: str
    accession_id: int
    footnote_type: Optional[int]
    SEC_URL: str
    sec_filing_id: Optional[int]
    blob_id: Optional[str]
    fiscal_year: int
    fiscal_period: Annotated[Period, BeforeValidator(_build_period)]
    calendar_year: int
    calendar_period: Annotated[Optional[Period], BeforeValidator(_build_period)]
    filing_date: str
    received_date: str
    document_type: str
    guide_link: Optional[str]
    page_url: Optional[str]
    entity_id: int
    id_detail: bool
    local_name: Optional[str]
    CIK: str
    sec_accession_number: Optional[str]
    network_id: Optional[int]
    ticker: str
    filing_type: int
    """
    Human readable disclosure name as reported by the filer, "Related Party Transactions", "Subsequent Events"
    """

    description: str
    """
    The name passed to the API, not set for 8-Ks, assigned by Calcbench.  We try to assign each disclosure to a category.
    """

    disclosure_type_name: str
    period_end_date: str
    footnote_type_title: FootnoteTypeTitle
    content: Optional[DisclosureContent] = None
    date_reported: Optional[datetime]
    """Time (EST) the document was available from Calcbench"""

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
        Content of the document, with the filers HTML
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
