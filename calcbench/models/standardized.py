from datetime import date, datetime
from typing import Any, Optional, Sequence, Union

from pydantic import BaseModel, Extra, validator
from calcbench.api_client import _try_parse_timestamp
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
    trace_facts: Optional[Sequence[TraceData]]
    ticker: str
    calcbench_entity_id: Optional[int]
    filing_type: Optional[str]
    """
    10-K, 10-Q, 8-K, PRESSRELEASE, etc.
    """
    preliminary: bool
    XBRL: Optional[bool]
    CIK: str
    trace_url: Optional[str]
    period: Any  # pandas period
    date_reported: Optional[datetime]
    """
    only on PIT points
    """
    period_start: Optional[date]
    period_end: Optional[date]
    confirming_XBRL_filing_ID: Optional[int]
    date_XBRL_confirmed: Optional[datetime]
    filing_accession_number: Optional[str]
    filing_id: Optional[int]
    date_modified: Optional[datetime]

    @validator("date_reported", "date_XBRL_confirmed", "date_modified", pre=True)
    def parse_date_reported(cls, value):
        return _try_parse_timestamp(value)

    @validator("period_start", "period_end", pre=True)
    def parse_period_start_end(cls, value):
        d = _try_parse_timestamp(value)
        if d:
            return d.date()
