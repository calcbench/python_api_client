from typing import Optional, Sequence
from pydantic import BaseModel, BeforeValidator
from typing_extensions import Annotated

from calcbench.api_query_params import Period
from calcbench.models.disclosure import _build_period


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
        """
        Unaltered HTML from the filing.
        """
        return "</br>".join(self.blobs)
