from datetime import date
from typing import Optional
from pydantic import BaseModel

from calcbench.api_query_params import Period


class Company(BaseModel, extra="allow"):
    ticker: str
    entity_name: str
    entity_id: int
    entity_code: str
    most_recent_filing: Optional[date] = None
    most_recent_full_year_end: Optional[date] = None
    most_recent_fiscal_year: Optional[int] = None
    most_recent_complete_calendar_year: Optional[int] = None
    most_recent_complete_fiscal_year: Optional[int] = None
    most_recent_filing_calendar_period: Period
    first_filing: Optional[date] = None
    naics_code: int
    sic_code: Optional[int] = None
    SICCategory: Optional[str] = None
    SICGroupMinorGroupTitle: Optional[str] = None
