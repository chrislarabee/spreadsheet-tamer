import pandas as pd

import datagenius.util as u


@u.transmutation(stage='explore')
def count_uniques(df: pd.DataFrame):
    """
    Counts the unique values in each column in the passed DataFrame.
    Null values are not counted.

    Args:
        df: A DataFrame.

    Returns: The DataFrame, and a metadata dictionary.

    """
    return df, {'metadata': pd.DataFrame(df.nunique()).T}


@u.transmutation(stage='explore')
def count_nulls(df: pd.DataFrame):
    """
    Counts the null values in each column in the passed DataFrame.

    Args:
        df: A DataFrame.

    Returns: The DataFrame, and a metadata dictionary.

    """
    return df, {'metadata': pd.DataFrame(df.isna().sum()).T}
