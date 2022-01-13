import logging
from pathlib import Path
from typing import Callable, Sequence, TypeVar

import calcbench as cb
import pandas as pd
from tqdm import tqdm

logging.getLogger("calcbench").setLevel(logging.DEBUG)
cb.enable_backoff(giveup=lambda e: e.response.status_code == 404)

T = TypeVar("T")


def iterate_and_save_pandas(
    arguments: Sequence[T], f: Callable[[T], pd.DataFrame], file_name: str
):
    """Apply arguments to a function that returns a DataFrame and save to a file.

    Usage::

    >>> %pip install calcbench-api-client[Pandas, Backoff] tqdm
    >>> from calcbench.downloaders import iterate_and_save_pandas
    >>> import calcbench as cb
    >>> tickers = cb.tickers(entire_universe=True)
    >>> iterate_and_save_pandas(
    >>>    tickers,
    >>>    lambda ticker: cb.point_in_time(
    >>>        all_face=True,
    >>>        all_footnotes=False,
    >>>        company_identifiers=[ticker],
    >>>        all_history=True,
    >>>        include_preliminary=True,
    >>>        include_xbrl=True,
    >>>    ),
    >>>    "fact_points.csv",
    >>> )

    """
    argument: T
    for argument in tqdm(arguments):
        try:
            data = f(argument)
            if data.empty:
                continue
        except KeyboardInterrupt:
            raise
        except Exception as e:
            tqdm.write(f"Exception getting {argument} {e}")
        else:
            file_exists = Path(file_name).exists()
            data.to_csv(
                file_name,
                mode="a" if file_exists else "w",
                index=False,
                header=not file_exists,
            )