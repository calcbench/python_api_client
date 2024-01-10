from datetime import date
from typing import TYPE_CHECKING, Sequence, Union

from pydantic import BaseModel


if TYPE_CHECKING:
    # https://github.com/microsoft/pyright/issues/1358
    import pandas as pd
else:
    try:
        import pandas as pd
    except ImportError:
        pass


def pydantic_to_pandas(items: Sequence[BaseModel]) -> "pd.DataFrame":
    """
    Convert pydantic objects to Pandas dataframe
    """
    if not items:
        return pd.DataFrame()
    items = list(items)
    if len(set(type(i) for i in items)) != 1:
        raise Exception("All items must be of the same type")
    df = pd.DataFrame([i.model_dump() for i in items]).convert_dtypes()
    model = items[0]
    for field, field_info in model.model_fields.items():
        annonation = field_info.annotation
        if (annonation == date) or (annonation == Union[date, type(None)]):
            df[field] = pd.to_datetime(df[field], errors="coerce")

    return df
