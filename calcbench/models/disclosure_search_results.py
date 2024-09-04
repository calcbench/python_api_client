from datetime import datetime
from typing import TYPE_CHECKING, Optional, Union
from typing_extensions import Annotated

from calcbench.models.disclosure_content import DisclosureContent
from calcbench.models.period import Period


if TYPE_CHECKING:
    # https://github.com/microsoft/pyright/issues/1358
    from bs4 import BeautifulSoup
else:
    try:
        from bs4 import BeautifulSoup
    except ImportError:
        pass


from pydantic import BaseModel, BeforeValidator, WrapValidator

from calcbench.api_client import _json_POST
from calcbench.models.disclosure import (
    FootnoteTypeTitle,
    _build_period,
    footnote_type_title_validator,
)


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

    def get_contents(self, standardize=False) -> str:
        """
        Content of the document, with the filers HTML

        :param standardize: Translate the contents into standardized/idiomatic HTML
        """
        return self.get_disclosure(standardize=standardize).contents

    def get_contents_text(self) -> str:
        """Contents of the HTML of the document"""
        return "".join(BeautifulSoup(self.get_contents(), "html.parser").strings)

    def get_disclosure(self, standardize: bool = False) -> DisclosureContent:
        """
        Get the disclosure contents from the Calcbench server

        :param standardize: Translate the contents into standardized/idiomatic HTML
        """
        if self.content:
            return self.content
        else:
            payload = DisclosureContentsParams(disclosure=self, standardize=standardize)
            json = _json_POST("disclosureContents", payload)
            return DisclosureContent(**json)

    def __str__(self):
        return f'DisclosureSearchResults(ticker="{self.ticker}", name="{self.name}", fiscal_year={self.fiscal_year}, fiscal_period={self.fiscal_period})'


class DisclosureContentsParams(BaseModel):
    """
    Get contents for a disclosure

    Corresponds DisclosureContentsParams.cs
    """

    disclosure: DisclosureSearchResults
    """
    Disclosure for which to get contents
    """

    standardize: Optional[bool] = False
    """
    Standardize the disclosure
    """
