from datetime import date
from typing import Optional
from unittest import TestCase
import numpy as np

from pydantic import BaseModel

from calcbench.pydantic_to_pandas import pydantic_to_pandas


class PydanticToPandas(TestCase):
    def test_date_conversion(self):
        """
        Optional data should become a datetime column
        """

        class X(BaseModel, extra="allow"):
            optional_date: Optional[date] = None

        l = [X(optional_date=None), X(optional_date=date.today())]
        df = pydantic_to_pandas(l)
        column = df["optional_date"]
        self.assertEqual(column.dtype, np.dtype("datetime64[ns]"))
