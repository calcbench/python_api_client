from datetime import date, datetime
from decimal import Decimal
from enum import Enum
from typing import Any, Optional, Sequence

from pydantic import BaseModel, Extra, validator


class StatementType(str, Enum):
    Income = "Income"
    Balance = "Balance"
    CashFlow = "CashFlow"
    ChangeInEquity = "StockholdersEquity"
    ComprehensiveIncome = "StatementOfComprehensiveIncome"


class SECLink(BaseModel, extra=Extra.allow):
    document_type: str
    """10-K, 8-K etc"""
    link: str
    """URL of the filing on Edgar"""
    filing_date: date
    """Date filing was received"""

    @validator("filing_date", pre=True)
    def parse_filing_date(cls, value):
        return datetime.strptime(value, "%m/%d/%Y").date()


class FinancialStatementColumn(BaseModel, extra=Extra.allow):
    fiscal_period: str
    '''Human readable, like "Y 2018"'''
    period_start: date
    period_end: date
    date_range: str
    '''Human readable, like "7/1/2017 to 6/30/2018"'''
    instant: bool
    """Does this refer to an instant in time or a period, balance sheet vs. income statment"""
    calculated: bool
    """Was this column calcuated by Calcbench, Q4 numbers for instance."""
    sec_links: Sequence[SECLink]
    is_guidance_column: bool
    """Is this column forward guidance"""
    fiscal_period_type: int
    fiscal_period_year: int
    fiscal_period_period: str


class Fact(BaseModel, extra=Extra.allow):
    fact_id: int
    effective_value: Decimal
    negated_label: str
    fact_value: Any
    unit_of_measure: str
    special_fact_type: str
    dimension_members: Optional[Sequence[str]]
    has_been_revised: bool
    revised: bool
    text_fact_id: Optional[int]
    focus: bool
    focus_negative: bool
    format_type: str


class LineItem(BaseModel, extra=Extra.allow):
    tree_depth: int
    type: str
    label: str
    local_name: str
    """XBRL tag"""
    is_subtotal: bool
    is_abstract: bool
    has_dimensions: bool
    unique_id: str
    is_extension: bool
    is_non_xbrl: bool
    normalized_point_name: str
    footnote_fact_id: Optional[int]
    facts: Optional[Sequence[Fact]]


class FaceStatement(BaseModel, extra=Extra.allow):
    entity_name: str
    name: str
    """The name of the statement from the filer"""
    columns: Sequence[FinancialStatementColumn]
    line_items: Sequence[LineItem]
