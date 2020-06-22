import pandas as pd

import datagenius.util as u


@u.transmutation(stage='reformat')
def reformat_df(
        df: pd.DataFrame,
        template: (list, pd.Index),
        mapping: dict) -> tuple:
    """
    Maps the passed DataFrame into a DataFrame matching the passed
    template based on the passed mapping dictionary. Unlike with basic
    pandas rename, the mapping values don't have to be 1 to 1. Pass a
    list or tuple as the value to be mapped to and the same column from
    df will be mapped to each of those columns in the output.

    Args:
        df: A DataFrame.
        template: A list or pandas Index representing the columns you
            want the output DataFrame to have.
        mapping: A dictionary with keys being columns in df and values
            being columns or lists/tuples of columns from template.

    Returns: A DataFrame with the columns in template and containing
        values mapped from the passed DataFrame. Also a metadata
        dictionary.

    """
    result = pd.DataFrame(columns=template)
    md = pd.DataFrame(df.count()).T
    for from_, to in mapping.items():
        if isinstance(to, str):
            result[to] = df[from_]
            md[from_] = to
        else:
            md[from_] = ''
            for i, t in enumerate(to):
                result[t] = df[from_]
                sep = ',' if i > 0 else ''
                md[from_] = md[from_] + sep + t
    result.columns, _ = u.standardize_header(result.columns)
    return result, {'metadata': md, 'orig_header': template}



