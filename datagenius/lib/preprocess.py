from typing import Optional, Sequence

import pandas as pd

import datagenius.element as e
import datagenius.util as u


def purge_pre_header(ds: e.Dataset) -> e.Dataset:
    """
    Removes any rows that appear before the header row in a Dataset
    where the header row wasn't the first row in the source data.
    Purged rows are stored in the Dataset's rejects attribute.

    Args:
        ds: A Dataset object.

    Returns: The Dataset object, cleaned of rows that came before the
        header, if any.

    """
    h = ds.header_idx
    if h:
        if h > 0:
            ds.rejects += [*ds.iloc[:h].values.tolist()]
        return ds.drop(index=[i for i in range(h)]).reset_index(drop=True)
    else:
        return ds


def detect_header(
        ds: e.Dataset,
        manual_header: Optional[Sequence] = None) -> e.Dataset:
    """
    Takes a Dataset object and sets its column names to be the
    values of the first row containing all true strings and removes
    that row from the Dataset.

    Args:
        ds: A Dataset object.
        manual_header: A Sequence of labels to use as column names.
            Useful when your Dataset has no discernible header.

    Returns: The Dataset, with the header row extracted as column
        names.

    """
    if manual_header:
        ds.columns = manual_header
    else:
        true_str_series = ds.apply(
            lambda x: u.count_true_str(x) == len(x), axis=1
        )
        first_idx = next(
            (i for i, v in true_str_series.items() if v), None)
        if first_idx is not None:
            ds.columns = list(ds.iloc[first_idx])
            ds.header_idx = first_idx
            return ds.drop(index=first_idx).reset_index(drop=True)
    return ds


def normalize_whitespace(ds: e.Dataset) -> e.Dataset:
    """
    Simple function that applies util.clean_whitespace to every cell
    in a Dataset.

    Args:
        ds: A Dataset.

    Returns: The Dataset, with any string values cleaned of excess
        whitespace.

    """
    for c in ds.columns:
        result = ds[c].apply(u.clean_whitespace)
        result = pd.DataFrame(result.to_list())
        ds[c] = result[1]
    return ds
