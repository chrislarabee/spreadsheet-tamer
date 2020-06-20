from typing import Optional, Sequence

import pandas as pd

import datagenius.util as u


@u.transmutation(stage='preprocess')
def purge_pre_header(df: pd.DataFrame, header_idx: int = None):
    """
    Removes any rows that appear before the header row in a DataFrame
    where the header row wasn't the first row in the source data.
    Purged rows are stored in the DataFrame's rejects attribute.

    Args:
        df: A pandas DataFrame object.
        header_idx: An integer, the index of the row where the header
            information was located.

    Returns: The DataFrame object, cleaned of rows that came before the
        header, if any.

    """
    if header_idx:
        rejects = pd.DataFrame()
        if header_idx > 0:
            rejects = df.iloc[:header_idx]
        df.drop(
            index=[i for i in range(header_idx)],
            inplace=True
        )
        df.reset_index(drop=True, inplace=True)
        return df, {'rejects': rejects}
    else:
        return df


@u.transmutation(stage='preprocess')
def detect_header(
        df: pd.DataFrame,
        manual_header: Optional[Sequence] = None) -> tuple:
    """
    Takes a pandas DataFrame and sets its column names to be the
    values of the first row containing all true strings and removes
    that row from the DataFrame.

    Args:
        df: A pandas DataFrame.
        manual_header: A Sequence of labels to use as column names.
            Useful when your DataFrame has no discernible header.

    Returns: A tuple containing the DataFrame, with the header row
        extracted as column names, and an integer indicating the idx
        where the header row was found (None if it was not pulled from
        the data).

    """
    header_idx = None
    if manual_header:
        header_idx = None
        df.columns = manual_header
    else:
        true_str_series = df.apply(
            lambda x: u.count_true_str(x) == len(x), axis=1
        )
        first_idx = next(
            (i for i, v in true_str_series.items() if v), None)
        if first_idx is not None:
            df.columns = list(df.iloc[first_idx])
            header_idx = first_idx
            df = df.drop(index=first_idx).reset_index(drop=True)
            return df, {'new_kwargs': dict(header_idx=header_idx)}
    return df, {'new_kwargs': dict(header_idx=header_idx)}


@u.transmutation(stage='preprocess')
def normalize_whitespace(df: pd.DataFrame) -> tuple:
    """
    Simple function that applies util.clean_whitespace to every cell
    in a DataFrame.

    Args:
        df: A DataFrame.

    Returns: The DataFrame, with any string values cleaned of excess
        whitespace.

    """
    md_df = pd.DataFrame([[0 for _ in df.columns]], columns=df.columns)
    for c in df.columns:
        result = df[c].apply(u.clean_whitespace)
        result = pd.DataFrame(result.to_list())
        df[c] = result[1]
        md_df[c] = result[0].sum()
    return df, {'metadata': md_df}
