from datagenius.names import name
from typing import Optional, List

import pandas as pd

from .namestring import Namestring
from .nametoken import Nametoken
import datagenius.util as u
from datagenius import config


def parse_name_string_column(
    df: pd.DataFrame, name_column: str, name_num: int = None
) -> pd.DataFrame:
    """
    Transmutation to parse a string in a single column in a DataFrame and break 
    it into its name components (prefix, fname, mname, lname, suffix). 
    -
    Args:
        df (pd.DataFrame): The DataFrame to transmute.
        name_column (str): The column label in df that contains the name to parse.
        name_num (int, optional): If your dataset has multiple names, you can 
            run this transmutation on it multiple times and increment this 
            argument to add a suffix to the column labels. Defaults to None.
    -
    Returns:
        pd.DataFrame: The passed DataFrame, with the data in the past column 
            broken out into 6 additional columns, the 5 columns specified in 
            datagenius' configuration, and an additional column 'valid' which 
            indicates whether the name in name_column is valid as a name.
    """
    suffix = str(name_num) if name_num else ""
    names = df[name_column].apply(Namestring)
    name_df = pd.DataFrame(
        names.apply(lambda n: n.to_list()).to_list(),
        columns=u.broadcast_suffix(config.name_columns, suffix)
    )
    name_df[f"valid{suffix}"] = names.apply(lambda n: n.valid)
    df = df.join(name_df)
    return df


def parse_tokenized_names(df: pd.DataFrame, name_columns: List[str]) -> pd.DataFrame:
    return df
