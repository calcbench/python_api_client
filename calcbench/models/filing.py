from datetime import datetime
from typing import Optional, Sequence
from pydantic import BaseModel, Extra, Field, validator
from calcbench.api_client import _try_parse_timestamp
from calcbench.api_query_params import Period

from calcbench.models.filing_type import FilingType
from calcbench.models.standardized import StandardizedPoint


class Filing(
    BaseModel,
    extra=Extra.allow,
):
    """A filing with the SEC or a wire press-release"""

    is_xbrl: Optional[bool] = Field(repr=False)
    is_wire: Optional[bool] = Field(repr=False)
    calcbench_id: int = Field(repr=False)
    """
    aka accession id
    """
    sec_accession_id: Optional[str] = Field(repr=False)
    sec_html_url: str = Field(repr=False)

    document_type: str
    """
    The label assigned to the filing by the SEC

    /FI,/MD,10-12B ,10-12B [Cover],10-12B/A [Amend] ,10-12B/A [Amend] [Cover],10-12G ,10-12G [Cover],10-12G/A,10-12G/A [Amend] ,10-12G/A [Amend] [Cover],10-D ,10-D/A [Amend] ,10-K,10-K [Cover],10-K [Paper],10-K/A,10-K/A [Amend] ,10-K/A [Amend] [Cover],10-KT ,10-KT/A,10-KT/A [Amend] ,10-KT/A [Amend] [Cover],10-Q,10-Q [Cover],10-Q [Paper],10-Q/A,10-Q/A [Amend] ,10-Q/A [Amend] [Cover],10-QT ,10-QT/A,10-QT/A [Amend] ,13F-HR,20-F,20-F/A,20FR12G,3,4,4/A,40-F,40-F/A,424B3,424B4,424B5,425,485APOS,485BPOS,485BXT,497,5,6-K,6-K/A,8-K,8-K [Cover],8-K/A,8-K/A [Amend] ,8-K/A [Amend] [Cover],8-K12B,8-K12B/A,8-K12B/A [Amend] ,8-K12G3,8-K12G3/A,8-K12G3/A [Amend] ,8-K15D5 ,ANNLRPT,Annual,ARS,CORRESP,DEF 14A,DEF 14C,DEFA14A,DEFC14A,DEFM14A,DEFM14C,DEFN14A,DEFR14A,DEFR14C,ESMA,F-1,F-1/A,F-3,F-3/A,F-3ASR,F-4,F-4/A,FOCUSN,HalfYear,MSD/A,MSDW,N-1A/A,NT 10-D ,NT 10-K,NT 10-K/A,NT 10-K/A [Amend] ,NT 10-Q,NT 10-Q/A,NT 10-Q/A [Amend] ,NT 11-K ,NT 20-F,NT 20-F/A,NT 20-F/A [Amend] ,NT N-CEN,NT N-MFP ,NT NPORT-EX,NT NPORT-P,NT-NCSR ,NT-NSAR ,NTN 10K ,NTN 10Q ,POS AM,POS EX,POSASR,PRE 14A,S-1,S-1/A,S-11,S-11/A,S-11MEF,S-1MEF,S-3,S-3/A,S-3ASR,S-4,S-4/A,SDR/A,SP 15D2,UPLOAD,WIREPR,X-17A-5,X-17A-5/A'
    """

    filing_type: FilingType
    """
    Standardized FilingType as assigned by Calcbench
    """

    filing_date: datetime
    fiscal_period: Optional[Period]
    fiscal_year: Optional[int]
    calcbench_accepted: datetime = Field(repr=False)
    calcbench_finished_load: Optional[datetime]
    entity_id: Optional[int] = Field(repr=False)
    ticker: Optional[str]
    entity_name: Optional[str] = Field(repr=False)
    CIK: Optional[str] = Field(repr=False)
    period_index: Optional[int] = Field(repr=False)
    associated_proxy_SEC_URL: Optional[str] = Field(repr=False)
    associated_earnings_press_release_SEC_URL: Optional[str] = Field(repr=False)
    period_end_date: Optional[datetime] = Field(repr=False)
    percentage_revenue_change: Optional[float] = Field(repr=False)
    this_period_revenue: Optional[float] = Field(repr=False)
    link1: Optional[str] = Field(repr=False)
    link2: Optional[str] = Field(repr=False)
    link3: Optional[str] = Field(repr=False)
    calendar_year: Optional[int] = Field(repr=False)
    calendar_period: Optional[Period] = Field(repr=False)
    standardized_XBRL: bool
    """
    Calcbench (should have) standardized data for this filing.
    """

    filing_id: int
    """
    preferred ID for filings.  

    corresponds to the ID column in Calcbench's SECFilings table
    """

    item_types: Optional[Sequence[str]] = Field(repr=False)
    """
    Item types for 8-Ks

    5.02,5.03,8.01,9.01 etc.
    """

    has_standardized_data: bool = Field(repr=False)
    """
    There is/should/will be, standardized data for this filing
    """

    @property
    def accession_id(self) -> int:
        """same as calcbench_id, calcbench_id should have been accession_id"""
        return self.calcbench_id

    @validator(
        "calcbench_finished_load",
        "calcbench_accepted",
        "filing_date",
        "period_end_date",
        pre=True,
    )
    def _parse_date(cls, value):
        return _try_parse_timestamp(value)

    def get_standardized_data(self, **args):
        """
        Standardized point-in-time data for this filing
        """
        args = {"filing_id": self.filing_id, "point_in_time": True, **args}
        return StandardizedPoint(**args)
