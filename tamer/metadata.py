from typing import Sequence, Any

import pandas as pd


class Metadata:
    def __init__(self) -> None:
        pass

    def collect(self, resolution_name: str, *, metadata=None, rejects=None, **unused):
        pass


def gen_empty_md_df(columns: Sequence, default_val: Any = 0) -> pd.DataFrame:
    """
    Generates an empty DataFrame with the passed columns and a one row
    placeholder. Used in resolution functions that need to accumulate metadata
    into an empty DataFrame.

    Args:
        columns (Sequence): Column labels to use in the empty df.
        default_val (Any): The default value to put in each column in the
            empty df.

    Returns:
        pd.DataFrame: A DataFrame with the passed columns and a single row
            containing a zero in each of those columns.
    """
    return pd.DataFrame([[default_val for _ in columns]], columns=columns)


METADATA = Metadata()
