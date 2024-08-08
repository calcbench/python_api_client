from enum import Enum
from typing import Any, Callable, Optional, Sequence, Union

from pydantic import BaseModel, ValidationError
from calcbench.api_query_params import Period


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


PERIOD_MAP = {
    "1Q": Period.Q1,
    "2Q": Period.Q2,
    "3Q": Period.Q3,
    "Y": Period.Annual,
    "1": Period.Q1,
    "2": Period.Q2,
    "3": Period.Q3,
    "4": Period.Q4,
    "Q1": Period.Q1,
    "Q2": Period.Q2,
    "Q3": Period.Q3,
    "Q4": Period.Q4,
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
    """
    Bad name, get all disclosure for one company/period, single company mode on the front-end.
    """
    disclosureNames: Optional[Sequence[str]] = None
    AllTextBlocks: Optional[bool] = False
    startOffset: Optional[int] = None
    allDisclosures: Optional[bool] = False
