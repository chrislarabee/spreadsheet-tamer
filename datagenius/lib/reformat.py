import pandas as pd

import datagenius.util as u


@u.transmutation(stage="reformat", priority=15)
def reformat_df(
    df: pd.DataFrame, reformat_template: (list, pd.Index), reformat_mapping: dict
) -> tuple:
    """
    Maps the passed DataFrame into a DataFrame matching the passed
    template based on the passed mapping dictionary. Unlike with basic
    pandas rename, the mapping values don't have to be 1 to 1. Pass a
    list or tuple as the value to be mapped to and the same column from
    df will be mapped to each of those columns in the output.

    Args:
        df: A DataFrame.
        reformat_template: A list or pandas Index representing the
            columns you want the output DataFrame to have.
        reformat_mapping: A dictionary with keys being columns in df
            and values being columns or lists/tuples of columns from
            template.

    Returns: A DataFrame with the columns in template and containing
        values mapped from the passed DataFrame. Also a metadata
        dictionary.

    """
    result = pd.DataFrame(columns=reformat_template)
    md = pd.DataFrame(df.count()).T
    for from_, to in reformat_mapping.items():
        if isinstance(to, str):
            result[to] = df[from_]
            md[from_] = to
        else:
            md[from_] = ""
            for i, t in enumerate(to):
                result[t] = df[from_]
                sep = "," if i > 0 else ""
                md[from_] = md[from_] + sep + t
    result.columns, _ = u.standardize_header(result.columns)
    return result, {"metadata": md, "orig_header": reformat_template}


@u.transmutation(stage="reformat")
def fill_defaults(df: pd.DataFrame, defaults_mapping: dict) -> tuple:
    """
    Fills each column specified in defaults_mapping with the values
    contained therein.

    Args:
        df: A DataFrame.
        defaults_mapping: A dictionary containing columns from df as
            keys and values being the value to fill nan cells in that
            column with.

    Returns: The passed DataFrame with null values filled in the
        columns specified with the values specified. Also a metadata
        dictionary.

    """
    md = u.gen_empty_md_df(df.columns)
    for k, v in defaults_mapping.items():
        md[k] = df[k].isna().sum()
        df[k] = df[k].fillna(v)
    return df, {"metadata": md}
