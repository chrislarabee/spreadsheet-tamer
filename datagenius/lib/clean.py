from typing import Sequence

import pandas as pd

import datagenius.util as u
import datagenius.element as e


@u.transmutation(stage='clean')
def complete_clusters(
        df: pd.DataFrame,
        clustered_columns: Sequence) -> tuple:
    """
    Forward propagates values in the given columns into nan values that
    follow non-nan values. Useful when you have a report-like dataset
    where the rows are clustered into groups where the columns that
    were grouped on aren't repeated if they're the same value.

    Args:
        df: A DataFrame.
        clustered_columns: The columns in the DataFrame to fill nan
            values with the last valid value.

    Returns: The DataFrame, with the passed columns forward filled with
        valid values instead of nans. Also a metadata dictionary.

    """
    md_df = u.gen_empty_md_df(clustered_columns)
    for c in clustered_columns:
        before_ct = df[c].count()
        df[c] = df[c].fillna(method='ffill')
        after_ct = df[c].count()
        md_df[c] = after_ct - before_ct
    return df, {'metadata': md_df}


@u.transmutation(stage='clean')
def reject_incomplete_rows(
        df: pd.DataFrame,
        required_cols: list) -> tuple:
    """
    Rejects any rows in a DataFrame that have nan values in the passed
    list of required columns.

    Args:
        df: A DataFrame.
        required_cols: A list (must be a list) of strings corresponding
            to the columns in df that must have values to be acceptable.

    Returns: The DataFrame, with only rows that have values in the
        required_cols list. Also a metadata dictionary.

    """
    metadata = dict()
    nulls = df.isna()
    nulls['count'] = nulls.apply(
        lambda row: row[required_cols].sum(), axis=1)
    incomplete_rows = nulls[nulls['count'] > 0].index
    rejects = df.iloc[incomplete_rows]
    metadata['rejects'] = rejects
    metadata['metadata'] = pd.DataFrame(rejects.count()).T
    df = df.drop(index=incomplete_rows)
    df = df.reset_index(drop=True)
    return df, metadata


def cleanse_typos(df: pd.DataFrame, **guides):
    """
    Corrects typos in the passed DataFrame based on keyword args where
    the key is the column and the arg is a dictionary of simple
    mappings or a CleaningGuide object.

    Args:
        df: A DataFrame.
        **guides: Keyword args where each key is a column name and each
            value is a dict or CleaningGuide object.

    Returns: The df, with the specified columns cleaned of typos, and a
        metadata dictionary.

    """
    results = u.gen_empty_md_df(df.columns)
    for k, v in guides.items():
        guides[k] = e.CleaningGuide.convert(v)

    for k, cl_guide in guides.items():
        new = df[k].apply(cl_guide)
        # nan != nan always evaluates to True, so need to subtract the
        # number of nans from the differing values:
        results[k] = (df[k] != new).sum() - df[k].isna().sum()
        df[k] = new

    return df, {'metadata': results}


# TODO: Rewrite cleanse_numeric_typos here.
