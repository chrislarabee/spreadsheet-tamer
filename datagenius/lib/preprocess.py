from typing import Optional, Sequence

import datagenius.element as e
import datagenius.util as u


def purge_pre_header(ds: e.Dataset) -> e.Dataset:
    """
    Removes any rows that appear before the header row in a Dataset
    where the header row wasn't the first row in the source data.

    Args:
        ds: A Dataset object.

    Returns: The Dataset object, cleaned of rows that came before the
        header, if any.

    """
    h = ds.meta_data.header_idx
    if h:
        if h > 0:
            ds.rejects += [*ds.iloc[:h].values.tolist()]
        return ds.drop(index=[i for i in range(h)])
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
        if first_idx:
            ds.columns = list(ds.iloc[first_idx])
            ds.meta_data.header_idx = first_idx
            return ds.drop(index=first_idx).reset_index(drop=True)
    return ds
