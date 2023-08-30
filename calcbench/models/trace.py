from typing import Optional, Union

from pydantic import BaseModel, Extra


class TraceData(BaseModel, extra=Extra.allow):
    local_name: str
    negative_weight: str
    XBRL_fact_value: Union[str, float, int]
    fact_id: Optional[int] = None
    dimensions: Optional[str] = None
