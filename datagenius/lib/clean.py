from typing import Sequence

import pandas as pd

import datagenius.util as u


def de_cluster(df: pd.DataFrame, columns: Sequence) -> tuple:
    """
    Forward propagates values in the given columns into nan values that
    follow non-nan values. Useful when you have a report-like dataset
    where the rows are clustered into groups where the columns that
    were grouped on aren't repeated if they're the same value.

    Args:
        df: A DataFrame.
        columns: The columns in the DataFrame to fill nan values with
            the last valid value.

    Returns: The DataFrame, with the passed columns forward filled with
        valid values instead of nans. Also a metadata dictionary.

    """
    md_df = pd.DataFrame([[0 for _ in columns]], columns=columns)
    for c in columns:
        before_ct = df[c].count()
        df[c] = df[c].fillna(method='ffill')
        after_ct = df[c].count()
        md_df[c] = after_ct - before_ct
    return df, {'metadata': md_df}

