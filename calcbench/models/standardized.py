from datetime import datetime
from typing import Optional, Sequence, Union

from pydantic import BaseModel
from calcbench.models.period import Period
from calcbench.models.trace import TraceData


class StandardizedPoint(BaseModel, extra="allow"):
    """
    Replicates MappedDataPoint on the server
    """

    metric: str
    """
    The metric name, see the definitions @ https://www.calcbench.com/home/standardizedmetrics
    """
    value: Union[str, float, int]
    """
    The value of the fact
    """

    original_value: Optional[float] = None
    """    
    The value that Calcbench extracted when it first processed the filing.

    Post November 2022, if this differs from the value Calcbench the fact was modified by Calcbench subsequent to the filing first being processed.
    """
    calendar_year: int
    """
    The calendar year for this fact.  https://knowledge.calcbench.com/hc/en-us/articles/223267767-What-are-Calendar-Years-and-Periods-What-is-TTM-
    """
    calendar_period: Period
    """
    The calendar period for this fact
    """
    fiscal_year: int
    fiscal_period: Period
    trace_facts: Optional[Sequence[TraceData]] = None
    """
    XBRL facts that went into the calculation of this point.  Specify `include_trace` for this field to be populated.
    """
    ticker: str
    """
    Ticker of reporting company
    """
    calcbench_entity_id: Optional[int] = None
    """
    Internal Calcbench identifier for reporting company
    """
    filing_type: Optional[str] = None
    """
    10-K, 10-Q, 8-K, PRESSRELEASE, etc.
    """
    preliminary: bool
    """
    True indicates the number was parsed from non-XBRL 8-K or press release from the wire
    """
    XBRL: Optional[bool] = None
    """
    Indicates the number was parsed from XBRL.

    The case where preliminary and XBRL are both true indicates the number was first parsed from a non-XBRL document then "confirmed" in an XBRL document.
    """
    CIK: str
    """
    SEC assigned Central Index Key for reporting company
    """
    trace_url: Optional[str] = None
    """
    URL for a page showing the source document for this value.
    """
    date_reported: Optional[datetime] = None
    """
    Timestamp (EST) when Calcbench finished processing the filing from which this value was parsed.

    In some cases, particularly prior to 2015, this will be the filing date of the document as recorded by the SEC.  To exclude these points remove points where the hour is 0.


    only on PIT points
    """
    period_start: Optional[datetime] = None
    """
    First day of the fiscal period for this fact
    """
    period_end: Optional[datetime] = None
    """
    Last day of the fiscal period for this fact
    """
    confirming_XBRL_filing_ID: Optional[int] = None
    """
    The filing_id of the XBRL document which contained this value.
    """
    date_XBRL_confirmed: Optional[datetime] = None
    """
    Time at which the point was confirmed by a point from an XBRL filing.
    If the point originally came from an XBRL filing this will be the original write time.  For values originally appearing in press-release, this will be the date of the associated 10-K/Q.
    If this is null, for points post April 2023, the point has not been confirmed.
    """
    filing_accession_number: Optional[str] = None
    """
    Accession number as assigned by the SEC for the filing from which this value came.
    """
    filing_id: Optional[int] = None
    date_modified: Optional[datetime] = None
    """
    The datetime Calcbench wrote/modified this value.

    Post November 2022 if this differs from the date_reported the fact was modified by Calcbench subsequent to the filing first being processed.
    """
    revision_number: Optional[int] = None
    """
    0 indicates an original, unrevised value for this fact. 1, 2, 3... indicates subsequent revisions to the fact value.  https://knowledge.calcbench.com/hc/en-us/search?utf8=%E2%9C%93&query=revisions&commit=Search
    """

    standardized_id: Optional[int] = None
    """
    A unique identifier Calcbench assigns to each standardized value.
    """
