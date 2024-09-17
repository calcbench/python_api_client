from typing import Optional, Union

from pydantic import BaseModel, Extra


class TraceData(BaseModel, extra=Extra.allow):
    local_name: Optional[str]
    """
    The XBRL tag
    """

    non_XBRL_label: Optional[str]
    """
    for points extracted from non-XBRL documents
    """

    negative_weight: bool
    XBRL_fact_value: Optional[Union[str, float, int]]
    fact_id: Optional[int] = None
    dimensions: Optional[str] = None
