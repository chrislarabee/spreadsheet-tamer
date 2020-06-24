import warnings
from collections import abc

import pandas as pd
import recordlinkage as link

import datagenius.util as u


class SupplementGuide(abc.MutableSequence):
    """
    A simple object used by supplement functions below to control what
    rules they use for creating and merging chunks of DataFrames.
    """
    def __init__(
            self,
            *on,
            conditions: dict = None,
            thresholds: (float, tuple) = None,
            block: (str, tuple) = None,
            inexact: bool = False):
        """

        Args:
            *on: An arbitrary list of strings, names of columns in the
                target DataFrame.
            conditions: A dictionary of conditions which rows in the
                target DataFrame must meet in order to qualify for this
                Supplement guide's instructions. Keys are column names
                and values are the value(s) in that column that qualify.
            thresholds: A float or tuple of floats of the same length
                as on. Used only if inexact is True, each threshold
                will be used with the on at the same index and matches
                in that column must equal or exceed the threshold to
                qualify as a match.
            block: A string or tuple of strings, column names in the
                target DataFrame. Use this if you're lucky enough to
                have data that you can match partially exactly on and
                just need inexact matches within that set of exact
                matches.
            inexact: A boolean, indicates whether this SupplementGuide
                represents exact or inexact match guidelines.
        """
        self.on: tuple = on
        c = {None: (None,)} if conditions is None else conditions
        for k, v in c.items():
            c[k] = u.tuplify(v)
        self.conditions: dict = c
        self.thresholds: tuple = u.tuplify(thresholds)
        self.block: tuple = u.tuplify(block)
        self.inexact: bool = inexact
        if self.inexact:
            if self.thresholds is None:
                self.thresholds = tuple([.9 for _ in range(len(self.on))])
            elif len(self.thresholds) != len(self.on):
                raise ValueError(
                    f'If provided, thresholds length must match on '
                    f'length: thresholds={self.thresholds}, on={self.on}')
        self.chunks: list = []

    def insert(self, index: int, x):
        self.chunks.insert(index, x)

    def output(self, *attrs) -> tuple:
        """
        Convenience method for quickly collecting a tuple of attributes
        from SupplementGuide.

        Args:
            *attrs: An arbitrary number of strings, which must be
                attributes in SupplementGuide. If no attrs are passed, output
                will just return on and conditions attributes.

        Returns: A tuple of attribute values.

        """
        if len(attrs) == 0:
            return self.on, self.conditions
        else:
            results = [getattr(self, a) for a in attrs]
            return results[0] if len(results) == 1 else tuple(results)

    def __getitem__(self, item: int):
        return self.chunks[item]

    def __setitem__(self, key: int, value: list):
        self.chunks[key] = value

    def __delitem__(self, key: int):
        self.chunks.pop(key)

    def __len__(self):
        return len(self.chunks)
    

def do_exact(df1: pd.DataFrame, df2: pd.DataFrame, on: tuple,
             rsuffix: str = '_s') -> pd.DataFrame:
    """
    Merges two DataFrames with overlapping columns based on exact
    matches in those columns.

    Args:
        df1: A pandas DataFrame.
        df2: A pandas DataFrame containing columns shared with df1.
        on: A tuple of columns shared by df1 and df2, which will be
            used to left join rows from df2 onto exact matches in
            df1.
        rsuffix: An optional suffix to use for overlapping columns
            outside the on columns. Will only be applied to df2
            columns.

    Returns: A DataFrame containing all the rows in df1, joined
        with any matched rows from df2.

    """
    return df1.merge(
        df2,
        'left',
        on=on,
        suffixes=('', rsuffix)
    )


def do_inexact(df1: pd.DataFrame, df2: pd.DataFrame, on: tuple,
               thresholds: tuple, block: tuple = None,
               rsuffix: str = '_s') -> pd.DataFrame:
    """
    Merges two DataFrames with overlapping columns based on inexact
    matches in those columns.

    Args:
        df1: A pandas DataFrame.
        df2: A pandas DataFrame containing columns shared with df1.
        on: A tuple of columns shared by df1 and df2, which will be
            used to left join rows from df2 onto inexact matches in
            df1.
        thresholds: A tuple of floats, indicating how close each on
            comparison must be to qualify the row as a match. Must
            be the same length as on.
        block: A tuple of columns shared by df1 and df2, similar to
            on, which must represent an exact match between the two
            frames. Useful when you can reduce the possible match
            space of two datasets by restricting inexact matches to
            records that at least have an exact match on a different
            column.
        rsuffix: An optional suffix to use for overlapping columns
            outside the on columns. Will only be applied to df2
            columns.

    Returns: A DataFrame containing all the rows in df1, joined
        with any matched rows from df2.

    """
    # The recordlinkage library is currently passing an argument
    # to the underlying jellyfish library that jellyfish is going
    # to deprecate eventually. Nothing we can do about that so just
    # suppress it:
    warnings.filterwarnings(
        'ignore', message="the name 'jaro_winkler'",
        category=DeprecationWarning)
    idxr = link.Index()
    idxr.block(block) if block is not None else idxr.full()
    candidate_links = idxr.index(df1, df2)
    compare = link.Compare()
    # Create copies since contents of the Dataframe need to
    # be changed.
    frames = (df1.copy(), df2.copy())

    for i, o in enumerate(on):
        compare.string(
            o, o, method='jarowinkler', threshold=thresholds[i])
        # Any columns containing strings should be lowercase to
        # improve matching:
        for f in frames:
            if f.dtypes[o] == 'O':
                f[o] = f[o].astype(str).str.lower()

    features = compare.compute(candidate_links, *frames)
    matches = features[features.sum(axis=1) == len(on)].reset_index()

    a = matches.join(df1, on='level_0', how='outer', rsuffix='')
    b = a.join(df2, on='level_1', how='left', rsuffix=rsuffix)
    drop_cols = ['level_0', 'level_1', *[i for i in range(len(on))]]
    b.drop(columns=drop_cols, inplace=True)
    return b


def chunk_dframes(plan: tuple, *frames) -> tuple:
    """
    Takes any number of pandas DataFrames and breaks each one into
    chunks based on a chunking plan of SupplementGuide objects.

    Args:
        plan: A tuple of SupplementGuide objects created by
            Supplement.build_plan, which will be used to chunk each
            DataFrame.
        *frames: An arbitrary number of pandas DataFrames, each of
            which must have the column labels named in the plan.

    Returns: Plan, with each SupplementGuide in the plan now having the
        chunk of rows that match its conditions, and the first
        DataFrame from frames, which contains any remaining rows
        that didn't match any of the conditions.

    """
    df1 = frames[0]
    for i, df in enumerate(frames):
        for p in plan:
            conditions = p.output('conditions')
            match, result = slice_dframe(df, conditions)
            p.append(match)
            if result:
                df.drop(match.index, inplace=True)
    return plan, df1


def slice_dframe(df: pd.DataFrame, conditions: dict) -> tuple:
    """
    Takes a dictionary of conditions in the form of:
        {'column_label': tuple(of, values, to, match)
    and returns a dataframe that contains only the rows that match
    all the passed conditions.

    Args:
        df: A pandas Dataframe containing the column_labels in
            conditions.keys()
        conditions: A dictionary of paired column_labels and tuples
            of values to match against.

    Returns: A DataFrame containing only the matching rows and a
        boolean indicating whether matching rows were found or if
        the DataFrame is simply being returned untouched.

    """
    df = df.copy()
    row_ct = df.shape[0]
    no_conditions = True
    for k, v in conditions.items():
        if k is not None:
            no_conditions = False
            df = df[df[k].isin(v)]
    new_ct = df.shape[0]
    result = True if (row_ct >= new_ct != 0
                      or no_conditions) else False
    return df, result


def build_plan(on: tuple) -> tuple:
    """
    Takes a tuple of mixed simple and complex on values and ensures
    they are standardized in the ways that chunk_dframes expects.

    Args:
        on: A tuple containing simple strings, tuples of
            dictionary and string/tuple pairs, or SupplementGuide
            objects.

    Returns: A tuple of SupplementGuide objects, one for each complex on
        and a single SupplementGuide for the simple ons at the end.

    """
    simple_ons = list()
    complex_ons = list()
    for o in on:
        if isinstance(o, SupplementGuide):
            complex_ons.append(o)
        elif isinstance(o, str):
            simple_ons.append(o)
        elif isinstance(o, tuple):
            pair = [None, None]
            for oi in o:
                if isinstance(oi, dict):
                    pair[1] = oi
                elif isinstance(oi, (str, tuple)):
                    pair[0] = u.tuplify(oi)
                else:
                    raise ValueError(
                        f'tuple ons must have a dict as one of '
                        f'their arguments and a str/tuple as the '
                        f'other Invalid tuple={o}'
                    )
            sg = SupplementGuide(*pair[0], conditions=pair[1])
            complex_ons.append(sg)
    if len(simple_ons) > 0:
        complex_ons.append(SupplementGuide(*simple_ons))
    return tuple(complex_ons)


def prep_ons(ons: (str, list, tuple)) -> tuple:
    """
    Ensures the passed ons are valid for use in build_plan.

    Args:
        ons: A string or list/tuple of strings/tuples.

    Returns: A tuple of valid ons.

    """
    if isinstance(ons, str):
        ons = u.tuplify(ons)
    elif isinstance(ons, list):
        ons = tuple(ons)
    elif isinstance(ons, tuple) and isinstance(ons[0], tuple):
        pass
    else:
        ons = (ons,)
    return ons


def prep_suffixes(suffixes: (str, tuple), frame_ct: int) -> tuple:
    """
    Ensures the passed suffixes are valid for use in
    GeniusAccessor.supplement.

    Args:
        suffixes: A string or tuple of strings.
        frame_ct: The number of other frames supplement is going to
            process.

    Returns: A tuple of valid suffixes.

    """
    if suffixes is None:
        suffixes = tuple(
            ['_' + a for a in u.gen_alpha_keys(frame_ct)])
    else:
        suffixes = u.tuplify(suffixes)
    if len(suffixes) != frame_ct:
        raise ValueError(f'Length of suffixes must be equal to the '
                         f'number of other frames. Suffix len='
                         f'{len(suffixes)}, suffixes={suffixes}')
    return suffixes
