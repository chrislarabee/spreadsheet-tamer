from typing import Sequence

import pandas as pd

import datagenius.util as u
import datagenius.element as e


@u.transmutation(stage='clean', priority=15)
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
    nulls = df.isna()
    nulls['count'] = nulls.apply(
        lambda row: row[required_cols].sum(), axis=1)
    incomplete_rows = nulls[nulls['count'] > 0].index
    rejects = df.iloc[incomplete_rows]
    df = df.drop(index=incomplete_rows)
    df = df.reset_index(drop=True)
    return df, u.package_rejects_metadata(rejects)


@u.transmutation(stage='clean')
def reject_on_conditions(
        df: pd.DataFrame,
        reject_conditions: (str, list, tuple)) -> tuple:
    """
    Takes a string or list/tuple of strings and uses them as a query to
    find matching rows in the passed DataFrame. The matching rows are
    then rejected.

    Args:
        df: A DataFrame.
        reject_conditions: A string or a list/tuple of strings, which
            must be valid conditions accepted by pandas.eval.

    Returns: The DataFrame, cleaned of rejected rows, as well as a
        metadata dictionary.

    """
    if not isinstance(reject_conditions, str):
        reject_conditions = ' & '.join(reject_conditions)
    rejects = df.query(reject_conditions)
    df = df.drop(index=rejects.index)
    df = df.reset_index(drop=True)
    return df, u.package_rejects_metadata(rejects)


@u.transmutation(stage='clean')
def reject_on_str_content(
        df: pd.DataFrame,
        reject_str_content: dict) -> tuple:
    """
    Takes a dictionary of column keys and search values and rejects
    any row in the passed DataFrame that has that search value in the
    string contained in that column. This is a stand alone function
    because pandas.query can't take in operators, so this kind of
    string parsing is not possible currently using that methodology.

    Args:
        df: A DataFrame
        reject_str_content: A dictionary of column names as keys and
            strings as a value to search within strings held in that
            column.

    Returns: The passed df, cleansed of rows that meet the rejection
        criteria in reject_str_content, as well as a metadata
        dictionary.

    """
    cond_results = pd.DataFrame()
    for k, v in reject_str_content.items():
        cond_results[k] = df[k].str.contains(v)
    matches = cond_results.any(axis=1)
    rejects = df.iloc[matches[matches].index]
    df = df.drop(index=rejects.index)
    df = df.reset_index(drop=True)
    return df, u.package_rejects_metadata(rejects)


@u.transmutation(stage='clean')
def cleanse_typos(df: pd.DataFrame, cleaning_guides: dict):
    """
    Corrects typos in the passed DataFrame based on keyword args where
    the key is the column and the arg is a dictionary of simple
    mappings or a CleaningGuide object.

    Args:
        df: A DataFrame.
        cleaning_guides: A dict where each key is a column name and
            each value is a dict or CleaningGuide object.

    Returns: The df, with the specified columns cleaned of typos, and a
        metadata dictionary.

    """
    results = u.gen_empty_md_df(df.columns)
    for k, v in cleaning_guides.items():
        cleaning_guides[k] = e.CleaningGuide.convert(v)

    for k, cl_guide in cleaning_guides.items():
        new = df[k].apply(cl_guide)
        # nan != nan always evaluates to True, so need to subtract the
        # number of nans from the differing values:
        results[k] = (df[k] != new).sum() - df[k].isna().sum()
        df[k] = new

    return df, {'metadata': results}


# TODO: Rewrite cleanse_numeric_typos here.


@u.transmutation(stage='clean')
def convert_types(df: pd.DataFrame, type_mapping: dict) -> tuple:
    """
    Uses the passed type_mapping dictionary to convert the indicated
    columns into the paired type object. Errors in type conversion
    will silently fail, so be sure to check types and maybe explore
    again to see if there are any pieces of data that failed to convert
    and give them additional attention.

    Args:
        df: A DataFrame.
        type_mapping: A dictionary containing column names as keys and
            python objects as values. Objects must be accepted by
            util.gconvert.

    Returns: The DataFrame, with the passed columns converted to the
        desired types, as well as a metadata dictionary.

    """
    md = u.gen_empty_md_df(df.columns)
    for col, type_ in type_mapping.items():
        result = df[col].apply(u.gconvert, args=(type_,))
        md[col] = (result.apply(type) != df[col].apply(type)).sum()
        df[col] = result

    return df, {'metadata': md}
