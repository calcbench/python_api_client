from typing import Optional, Union

from pydantic import BaseModel, Extra


class TraceData(BaseModel, extra=Extra.allow):
    local_name: Optional[str]
    negative_weight: bool
    XBRL_fact_value: Optional[Union[str, float, int]]
    fact_id: Optional[int] = None
    dimensions: Optional[str] = None
