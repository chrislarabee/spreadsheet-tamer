from typing import Tuple

import pandas as pd

from .decorators import resolution
from . import metadata as md
from .type_handling import CollectibleMetadata


@resolution
def complete_clusters(
    df: pd.DataFrame, *clustered_columns: Tuple[str, ...]
) -> Tuple[pd.DataFrame, CollectibleMetadata]:
    """
    Forward propagates values in the given columns into nan values that follow
    non-nan values.

    So complete_clusters(df, "a", "b"):
        a   b   c           a   b   c
    0   1   2   3       0   1   2   3
    1   nan 2   nan ->  1   1   2   nan
    2   4   5   6       2   4   5   6
    3   nan nan nan     3   4   5   nan

    Args:
        df (pd.DataFrame): A DataFrame.
        clustered_columns: The columns in the DataFrame to fill nan values with
            the last valid value.

    Returns:
        Tuple[pd.DataFrame, CollectibleMetadata]: The modified DataFrame, and a
            metadata dictionary.
    """
    md_df = md.gen_empty_md_df(clustered_columns)
    for c in clustered_columns:
        before_ct = df[c].count()
        df[c] = df[c].fillna(method="ffill")
        after_ct = df[c].count()
        md_df[c] = after_ct - before_ct
    return df, dict(metadata=md_df)
