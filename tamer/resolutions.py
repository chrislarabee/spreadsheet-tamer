from typing import Tuple, Dict, Union, Any

import pandas as pd
import numpy as np

from .decorators import resolution
from . import metadata as md
from .type_handling import CollectibleMetadata
from . import iterutils


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


@resolution
def fillna_shift(df: pd.DataFrame, *column_fill_order: Tuple[str, ...]) -> pd.DataFrame:
    """
    Takes at least two columns in the DataFrame and shifts all their values
    "leftward", replacing nan values. Basically, a given column's value will be
    moved "left" in the reverse of the order specified by columns until it hits
    the end or a non-null value.

    So:
        a   b   c            a   b   c
    0   1   nan 2        0   1   2   nan
    1   nan 3   4   ->   1   3   4   nan
    2   nan nan 5        2   5   nan nan

    If you pass a, b, c in that order. c, b, a would reverse the direction of the
    shift. You can also do arbitrary column orders like b, c, a.

    Args:
        df (pd.DataFrame): A DataFrame.
        columns: The columns in the DataFrame to shift values along and fill
            nan cells.

    Returns:
        pd.DataFrame: The modified DataFrame.
    """
    if len(column_fill_order) < 2:
        raise ValueError("Must supply at least 2 columns.")
    for i, c in enumerate(column_fill_order[:-1]):
        for c2 in column_fill_order[i + 1 :]:
            df[c].fillna(df[c2], inplace=True)
            df[c2] = np.where(df[c] == df[c2], np.nan, df[c2])
    return df


@resolution
def remove_redundancies(
    df: pd.DataFrame, redundancy_map: Dict[str, Union[str, Tuple[str, ...]]]
) -> Tuple[pd.DataFrame, CollectibleMetadata]:
    """
    For each row in the DataFrame, if a key in redundancy_map contains the same
    value as the column(s) in the paired value, replaces the column(s)' value
    with nan, removing the redundant data.

    Args:
        df (pd.DataFrame): A DataFrame.
        redundancy_map (Dict[str, Union[str, Tuple[str, ...]]]): A dictionary
            with master column names as keys (the columns that *should* contain
            the data) and one or more other columns that may also contain the
            value in the master column.

    Returns:
        Tuple[pd.DataFrame, CollectibleMetadata]: The modified DataFrame, and a
            metadata dictionary.
    """
    for k, v in redundancy_map.items():
        redundancy_map[k] = iterutils.tuplify(v)
    md_df = md.gen_empty_md_df(df.columns)
    for master, extras in redundancy_map.items():
        for e in extras:
            result = df.apply(
                lambda row: np.nan if row[master] == row[e] else row[e], axis=1
            )
            md_df[e] = df[e].count() - result.count()
            df[e] = result
    return df, dict(metadata=md_df)