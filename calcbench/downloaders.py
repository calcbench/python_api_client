from typing import Callable, List, Literal, Optional, Sequence, TypeVar, Union
from pathlib import Path


import calcbench as cb
import pandas as pd
from tqdm.auto import tqdm


cb.enable_backoff(
    giveup=lambda e: hasattr(e, "response") and (e.response.status_code in [404, 500])
)

T = TypeVar("T")


def iterate_to_dataframe(
    arguments: Sequence[T],
    f: Callable[[T], pd.DataFrame],
) -> pd.DataFrame:
    """Apply arguments to a function that returns a DataFrame append to a dataframe and return.

    Usage::

    >>> %pip install calcbench-api-client[Pandas,Backoff,tqdm]
    >>> from calcbench.downloaders import iterate_to_dataframe
    >>> import calcbench as cb
    >>> tickers = cb.tickers(entire_universe=True)
    >>> d = iterate_and_save_pandas(
    >>>    tickers,
    >>>    lambda ticker: cb.point_in_time(
    >>>        all_face=True,
    >>>        all_footnotes=False,
    >>>        company_identifiers=[ticker],
    >>>        all_history=True,
    >>>        include_preliminary=True,
    >>>        include_xbrl=True,
    >>>    ),
    >>> )
    """
    argument: T
    chunks = []
    for argument in tqdm(list(arguments)):
        try:
            d = f(argument)
            if d.empty:
                continue
        except KeyboardInterrupt:
            raise
        except Exception as e:
            tqdm.write(f"Exception getting {argument} {e}")
        else:
            chunks.append(d)
    if chunks:
        return pd.concat(chunks)
    else:
        return pd.DataFrame()


def iterate_and_save_pandas(
    arguments: Sequence[T],
    f: Callable[[T], pd.DataFrame],
    file_name: Union[str, Path],
    write_index: bool = True,
    columns: Optional[Sequence[str]] = None,
    write_mode: Literal["w", "a"] = "w",
):
    """Apply arguments to a function that returns a DataFrame and save to a .csv file.

    :param arguments: Each item in this sequence will be passed to f
    :param f: Function that generates a pandas dataframe that will be called on arguments
    :param file_name: Name of the file to write
    :param write_index: Write the pandas index to the csv file
    :param columns: which columns to write.  If this is set the index is not written
    :param write_mode: set the initial write mode.  "a" to append, "w" to overwrite.  Useful for resuming downloading.

    Usage::

    >>> %pip install calcbench-api-client[Pandas,Backoff,tqdm]
    >>> from calcbench.downloaders import iterate_and_save_pandas
    >>> import calcbench as cb
    >>> tickers = cb.tickers(entire_universe=True)
    >>> iterate_and_save_pandas(
    >>>    arguments=tickers,
    >>>    f=lambda ticker: cb.standardized(company_identifiers=[ticker], point_in_time=True),
    >>>    file_name="calcbench_standardized_PIT.csv",
    >>> )

    """
    argument: T
    write_headers = write_mode != "a"
    for argument in tqdm(list(arguments)):
        try:
            data = f(argument)
            if data.empty:
                continue
        except KeyboardInterrupt:
            raise
        except Exception as e:
            tqdm.write(f"Exception getting {argument} {e}")
        else:
            if columns:
                try:
                    data = data.reset_index()[columns]
                except Exception as e:
                    tqdm.write(f"Exception getting columns for {argument} {e}")
                    continue
            data.to_csv(
                file_name,
                mode=write_mode,
                index=write_index,
                header=write_headers,
            )
            write_mode = "a"
            write_headers = False


def iterate_and_save_pyarrow_dataset(
    arguments: Sequence[T],
    f: Callable[[T], pd.DataFrame],
    root_path: Union[str, Path],
    partition_cols: Optional[List[str]] = ["ticker"],
):
    """
    Apply the arguments to a function a save to a pyarrow dataset.

    Usage::
    >>> tickers = sorted(cb.tickers(entire_universe=True), key=lambda ticker: hash(ticker)) # randomize the order so the time estimate is better
    >>>
    >>> iterate_and_save_pyarrow_dataset(
    >>>     arguments=tickers,
    >>>     f=lambda ticker: cb.standardized(company_identifiers=[ticker], point_in_time=True),
    >>>     root_path="~/standardized_PIT_arrow/",
    >>>     partition_cols=["ticker"],
    >>>)
    >>>
    >>> # Read the dataset
    >>> import pyarrow.parquet as pq
    >>> import pyarrow.compute as pc
    >>> table = pq.read_table(<root_path>)
    >>> expr = pc.field("ticker") == "MSFT"
    >>> msft_data = table.filter(expr).to_pandas()

    """
    import pyarrow as pa
    import pyarrow.parquet as pq

    for argument in tqdm(list(arguments)):
        df = f(argument)
        if df.empty:
            continue
        table = pa.Table.from_pandas(df)
        pq.write_to_dataset(
            table=table,
            root_path=root_path,
            partition_cols=partition_cols,
            **{"allow_truncated_timestamps": True, "coerce_timestamps": "us"},
        )