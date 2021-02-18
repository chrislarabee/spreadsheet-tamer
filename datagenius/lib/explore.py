import warnings

import pandas as pd

import datagenius.util as u

warnings.warn("datagenius.lib.explore is deprecated.", DeprecationWarning)


@u.transmutation(stage="explore")
def count_values(df: pd.DataFrame):
    """
    Counts the values in each column in the passed DataFrame.
    Null values are not counted.

    Args:
        df: A DataFrame.

    Returns: The DataFrame, and a metadata dictionary.

    """
    return df, {"metadata": pd.DataFrame(df.count()).T}


@u.transmutation(stage="explore")
def count_uniques(df: pd.DataFrame):
    """
    Counts the unique values in each column in the passed DataFrame.
    Null values are not counted.

    Args:
        df: A DataFrame.

    Returns: The DataFrame, and a metadata dictionary.

    """
    md = u.gen_empty_md_df(df.columns)
    # This loop avoids using nunique on raw data, which can cause
    # errors if the data contains unexpected data types like lists.
    # Using gconvert to convert to string before counting uniques
    # prevents errors.
    for c in df.columns:
        md[c] = df[c].apply(u.gconvert, target_type=str).nunique()
    return df, {"metadata": md}


@u.transmutation(stage="explore")
def count_nulls(df: pd.DataFrame):
    """
    Counts the null values in each column in the passed DataFrame.

    Args:
        df: A DataFrame.

    Returns: The DataFrame, and a metadata dictionary.

    """
    return df, {"metadata": pd.DataFrame(df.isna().sum()).T}


@u.transmutation(stage="explore")
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
    dtypes["ctr"] = 1
    result = u.gen_empty_md_df(df.columns)
    for c in orig_cols:
        c_pcts = (dtypes.groupby([c]).sum() / dtypes[c].count()).round(2)
        c_pcts = c_pcts.reset_index()
        result[c] = ",".join(
            c_pcts.apply(lambda s: f"{s[c]}({s.ctr})", axis=1).tolist()
        )
    return df, {"metadata": result}


@u.transmutation(stage="violations")
def id_type_violations(df: pd.DataFrame, required_types: dict) -> tuple:
    """
    Checks if each value in the columns specified in the passed dict
    is an object of the passed type. Note that nan values will always
    count as matching the passed type, see id_nullable_violations
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
    return df, {"metadata": result}


@u.transmutation(stage="violations")
def id_nullable_violations(df: pd.DataFrame, not_nullable: (list, tuple)) -> tuple:
    """
    Checks if each column in not_nullable contains no nulls.

    Args:
        df: A DataFrame.
        not_nullable: A list of columns in df that shouldn't contain
            nan values.

    Returns: The DataFrame, and a metadata dictionary.

    """
    result = u.gen_empty_md_df(df.columns, False)
    nulls = pd.DataFrame(df.isna().sum()).T
    for col in not_nullable:
        result[col] = nulls[col] > 0
    return df, {"metadata": result}


@u.transmutation(stage="violations")
def id_clustering_violations(
    df: pd.DataFrame, cluster_group_by: list, cluster_unique_cols: list
) -> tuple:
    """
    Clusters are sets of rows that share one or more identical columns
    and have another set of columns which must be unique within the
    cluster. This function identifies rows that are part of a cluster
    that violates these rules.

    Args:
        df: A DataFrame.
        cluster_group_by: A list of columns in df that, when grouped,
            define a cluster.
        cluster_unique_cols: A list of columns in df that, when the df
            is grouped on cluster_group_by, must be unique within the
            cluster. If you want combinations of columns to be unique,
            pass them as a tuple within cluster_unique_cols.

    Returns: The DataFrame, with each row appended with details about
        whether it violates clustering, and how. Also a metadata
        dictionary.

    """
    md = u.gen_empty_md_df(df.columns)
    df["row_ct"] = 1
    # Preprocess cluster_unique_cols to handle column combinations:
    u_col_names = []
    cols_to_count = []
    for c in cluster_unique_cols:
        if isinstance(c, tuple):
            cols_to_count += [*c]
            name = "_".join(c) + "_x"
            df[name] = ""
            for source_col in c:
                df[name] = df[name] + df[source_col].astype(str)
            u_col_names.append(name)
        else:
            u_col_names.append(c)
            cols_to_count.append(c)
    # Reset_index twice so each cluster has a unique id:
    cluster_row_cts = (
        df.groupby(cluster_group_by)["row_ct"].sum().reset_index().reset_index()
    )
    cluster_row_cts.rename(columns={"index": "cluster_id"}, inplace=True)
    # row_ct in core df no longer necessary:
    df = df.drop(columns="row_ct")
    # Get the count of each unique value in the ungrouped columns:
    nu = df.groupby(cluster_group_by).nunique().reset_index()
    # Get the count of each value in the ungrouped columns:
    ct = df.groupby(cluster_group_by).count().reset_index()
    # Combine row_cts, unique counts, and counts into core df:
    clusters = (
        df.merge(cluster_row_cts, how="left", on=cluster_group_by)
        .merge(
            nu[[*cluster_group_by, *u_col_names]],
            on=cluster_group_by,
            how="left",
            suffixes=("", "_nu"),
        )
        .merge(
            ct[[*cluster_group_by, *cols_to_count]],
            on=cluster_group_by,
            how="left",
            suffixes=("", "_ct"),
        )
    )
    # Apply a row_number to each row within each cluster:
    clusters["rn"] = clusters.groupby([*cluster_group_by, "cluster_id"]).cumcount() + 1
    # Unique columns and column combinations must be unique:
    for c in u_col_names:
        result = clusters[c + "_nu"] != clusters["row_ct"]
        md[c] = result.sum()
        clusters[c + "_invalid"] = result
    # Unique columns must either have no values or be fully not-null:
    for c in cols_to_count:
        result = (clusters["row_ct"] != clusters[c + "_ct"]) & (
            clusters[c + "_ct"] != 0
        )
        md[c] = result.sum()
        clusters[c + "_invalid"] = result
    # Rows that fail any of the above tests are invalid:
    invalid_inds = u.broadcast_suffix(list({*u_col_names, *cols_to_count}), "_invalid")
    clusters["cluster_invalid"] = clusters[invalid_inds].any(axis=1)
    return clusters, {"metadata": md}
