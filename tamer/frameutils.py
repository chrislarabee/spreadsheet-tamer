from typing import Callable, Any, Union, Tuple, List
import pandas as pd
from numpy import nan

from . import iterutils


def accrete(
    df: pd.DataFrame,
    accrete_group_by: List[str],
    accretion_cols: Union[str, Tuple[str, ...]],
    accretion_sep: str = " ",
) -> pd.DataFrame:
    """
    Groups the dataframe by the passed group_by values and then concatenates
    values in the accretion columns with the passsed seperator between them.

    Args:
        df (pd.DataFrame): A DataFrame
        accrete_group_by (List[str]): A list of labels, columns to group df by.
        accretion_cols (Union[str, Tuple[str, ...]]): The columns you want to
            concatenate within each group resulting from accrete_group_by.
        accretion_sep (str, optional): The value to separate the concatenated
            values. Defaults to " ".

    Returns:
        pd.DataFrame: The modified DataFrame.
    """
    accretion_cols = iterutils.tuplify(accretion_cols)
    for c in accretion_cols:
        df[c] = df[c].fillna("")
        df[c] = df[c].astype(str)
        result = df.groupby(accrete_group_by)[c].apply(accretion_sep.join).reset_index()
        df = df.merge(result, on=accrete_group_by, suffixes=("", "_x"))
        cx = c + "_x"
        df[c] = df[cx]
        df.drop(columns=cx, inplace=True)
        df[c] = df[c].str.strip()
        df[c] = df[c].apply(
            lambda x: x if len(x) > 0 and x[-1] != accretion_sep else x[:-1]
        )
        df[c] = df[c].replace("", nan)
    return df


def multiapply(
    df: pd.DataFrame,
    *columns: str,
    func: Callable[[Any], Any] = None,
    **column_func_pairs: Callable[[Any], Any]
) -> pd.DataFrame:
    """
    Convenience function for applying a variety of single argument functions to
    various combinations of columns of a DataFrame. You can broadcast a single
    function over multiple columns, or select specific columns to apply a
    specific function to, or both.

    Note that if you apply a function to a column that contains nan values or
    mixed datatypes, you need to make sure your function can handle those
    scenarios. Wrapping your function in @nullable solves the nan problem at
    least.

    Args:
        df (pd.DataFrame): A DataFrame
        *columns (str): Arbitrary column labels from df.
        func (Callable[[Any], Any], optional): A function to apply to the values
            in df[columns]. Defaults to None.
        **column_func_pairs [Callable[[Any], Any]]: Arbitrary column labels from
            df and the accompanying function that should be applied to them.

    Returns:
        pd.DataFrame: The modified DataFrame.
    """
    col_func_map = column_func_pairs
    if func is not None:
        col_func_map = {**column_func_pairs, **{c: func for c in columns}}
    for column, f in col_func_map.items():
        df[column] = df[column].apply(f)
    return df
