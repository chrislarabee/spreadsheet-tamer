from typing import List

import pandas as pd

from .namestring import Namestring
from .nametoken import Nametoken  # noqa: F401
import datagenius.util as u
from tamer.config import config


def parse_name_string_column(
    df: pd.DataFrame,
    name_column: str,
    name_num: int = None,
    include_name2: bool = False,
) -> pd.DataFrame:
    """
    Transmutation to parse a string in a single column in a DataFrame and break
    it into its name components (prefix, fname, mname, lname, suffix).

    Args:
        df (pd.DataFrame): The DataFrame to transmute.
        name_column (str): The column label in df that contains the name to parse.
        name_num (int): If your dataset has multiple names, you can
            run this transmutation on it multiple times and increment this
            argument to add a suffix to the column labels. Defaults to None.
        include_name2 (bool): If name_column includes two people (e.g. Bob and
            Helen Parr), set this to True if you want to have the second name
            included in the appended names as well. Defaults to False.

    Returns:
        pd.DataFrame: The passed DataFrame, with the data in the past column
            broken out into 6 additional columns, the 5 columns specified in
            datagenius' configuration, and an additional column 'valid' which
            indicates whether the name in name_column is valid as a name.
    """
    suffix = str(name_num) if name_num else ""
    suffix2 = str(name_num + 1) if name_num else "2"
    names = df[name_column].apply(Namestring)
    labels = u.broadcast_suffix(config.name_column_labels, suffix)
    if include_name2:
        labels += u.broadcast_suffix(config.name_column_labels, suffix2)
    name_df = pd.DataFrame(
        names.apply(lambda n: n.to_list(force_name2=include_name2)).to_list(),
        columns=labels,
    )
    name_df["valid"] = names.apply(lambda n: n.valid)
    df = df.join(name_df)
    return df


# TODO: Implement parse_tokenized_names:
def parse_tokenized_names(df: pd.DataFrame, name_columns: List[str]) -> pd.DataFrame:
    return df
