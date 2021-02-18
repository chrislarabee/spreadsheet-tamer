from typing import Callable
from typing import Callable, Any
import pandas as pd


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
        pd.DataFrame: [description]
    """
    col_func_map = column_func_pairs
    if func is not None:
        col_func_map = {**column_func_pairs, **{c: func for c in columns}}
    for column, f in col_func_map.items():
        df[column] = df[column].apply(f)
    return df
