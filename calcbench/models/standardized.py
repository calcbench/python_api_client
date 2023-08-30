from datetime import datetime
from typing import Optional, Sequence, Union

from pydantic import BaseModel, Extra
from calcbench.api_query_params import Period
from calcbench.models.trace import TraceData


class StandardizedPoint(BaseModel, extra=Extra.allow):
    """
    Replicates MappedDataPoint on the server
    """

    metric: str
    value: Union[str, float, int]
    calendar_year: int
    calendar_period: Period
    fiscal_year: int
    fiscal_period: Period
    trace_facts: Optional[Sequence[TraceData]] = None
    ticker: str
    calcbench_entity_id: Optional[int] = None
    filing_type: Optional[str] = None
    """
    10-K, 10-Q, 8-K, PRESSRELEASE, etc.
    """
    preliminary: bool
    XBRL: Optional[bool] = None
    CIK: str
    trace_url: Optional[str] = None
    date_reported: Optional[datetime] = None
    """
    only on PIT points
    """
    period_start: Optional[datetime] = None
    period_end: Optional[datetime] = None
    confirming_XBRL_filing_ID: Optional[int] = None
    date_XBRL_confirmed: Optional[datetime] = None
    filing_accession_number: Optional[str] = None
    filing_id: Optional[int] = None
    date_modified: Optional[datetime] = None
