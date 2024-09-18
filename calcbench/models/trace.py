from typing import Optional, Union

from pydantic import BaseModel

from calcbench.models.period import Period


class TraceData(BaseModel, extra="allow"):
    local_name: Optional[str] = None
    """
    The XBRL tag, null for non-XBRL facts
    """

    non_XBRL_label: Optional[str] = None
    """
    for points extracted from non-XBRL documents
    """

    negative_weight: bool
    XBRL_fact_value: Optional[Union[str, float, int]] = None
    fact_id: Optional[int] = None
    """
    calcbench XBRL fact ID
    """
    dimensions: Optional[str] = None

    fiscal_period: Optional[Period] = None
    """
    fiscal period for this fact
    """
    fiscal_year: Optional[int] = None
    """
    fiscal year for this fact
    """
