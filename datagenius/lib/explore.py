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


@u.transmutation(stage='explore')
def collect_data_types(df: pd.DataFrame):
    """
    Collects the unique python data types in the passed DataFrame's
    columns and assembles a string of each unique type with the percent
    of values that type represents in that column.

    Args:
        df: A DataFrame.

    Returns: The DataFrame, and a metadata dictionary.

    """
    dtypes = df.applymap(u.get_class_name)
    orig_cols = list(dtypes.columns)
    dtypes['ctr'] = 1
    result = u.gen_empty_md_df(df.columns)
    for c in orig_cols:
        c_pcts = dtypes.groupby([c]).sum() / dtypes[c].count()
        c_pcts = c_pcts.reset_index()
        result[c] = ','.join(
            c_pcts.apply(
                lambda s: f'{s[c]}({s.ctr})', axis=1).tolist())
    return df, {'metadata': result}


@u.transmutation(stage='explore')
def check_type_violations(
        df: pd.DataFrame,
        required_types: dict) -> tuple:
    """
    Checks if each value in the columns specified in the passed dict
    is an object of the passed type. Note that nan values will always
    count as matching the passed type, see check_nullable_violations
    to find erroneous nulls.

    Args:
        df: A DataFrame.
        required_types: A dictionary containing keys corresponding to
            columns in df, and values corresponding to the python type
            you want each value in that column to be.

    Returns: The DataFrame, and a metadata dictionary.

    """
    result = u.gen_empty_md_df(df.columns, False)
    types = df.applymap(u.gtype)
    for col, type_ in required_types.items():
        types[col] = types[col].fillna(type_)
        result[col] = (types[col] != type_).sum() > 0
    return df, {'metadata': result}
