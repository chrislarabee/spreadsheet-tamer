from typing import Callable, Any, Optional, Union, Tuple, List, Dict, Sequence
from collections.abc import MutableSequence
import warnings

import pandas as pd
from numpy import nan, select
import recordlinkage as link

from . import iterutils


class ComplexJoinRule(MutableSequence):
    def __init__(
        self,
        *on: str,
        conditions: dict = None,
        thresholds: Union[float, Tuple[float, ...]] = None,
        block: Union[str, Tuple[str, ...]] = None,
    ):
        """
        A rule

        Args:
            *on: An arbitrary list of strings, names of columns in the target
                DataFrame.
            conditions (Dict[str, Any], optional): A dictionary of conditions
                which rows in the target DataFrame must meet in order to qualify
                for this ComplexJoinRule's instructions. Keys are column names
                and values are the value(s) in that column that qualify. Defaults
                to None.
            thresholds (Union[float, Tuple[float, ...]], optional): Each
                threshold will be paired with the on value at the corresponding
                index for inexact value matching. Match values must be equal to
                or greater than the threshold to count as a match. If you want to
                use a single threshold for all ons, pass a single float. Defaults
                to None.
            block (Union[str, Tuple[str, ...]], optional): Column names in the
                target DataFrame to require exact matches on. Defaults to None.

        Raises:
            ValueError: If threshold values are passed and the # of thresholds
                passed does not match the # of ons patched.
        """
        self.chunks = []
        self.on = on
        self._conditions = (
            {k: iterutils.tuplify(v) for k, v in conditions.items()}
            if conditions
            else {None: (None,)}
        )
        self.block = iterutils.tuplify(block) if block else None
        if isinstance(thresholds, float):
            self._thresholds = tuple([thresholds for _ in range(len(self.on))])
        elif isinstance(thresholds, Sequence):
            self._thresholds = iterutils.tuplify(thresholds)
            if len(self._thresholds) != len(self.on):
                raise ValueError(
                    f"If provided, thresholds length must match on length: "
                    f"thresholds={self._thresholds}, on={self.on}"
                )
        else:
            self._thresholds = None

    @property
    def conditions(self) -> dict:
        return self._conditions

    @property
    def thresholds(self) -> Optional[Tuple[float, ...]]:
        return self._thresholds

    def insert(self, index: int, x):
        self.chunks.insert(index, x)

    def output(self, *attrs: str) -> Tuple[Any, ...]:
        """
        Convenience method for quickly collecting a tuple of attributes from the
        ComplexJoinRule.

        Args:
            *attrs: An arbitrary number of strings, which must be attributes
                in the ComplexJoinRule. If no attrs are passed, output will just
                return on and conditions attributes.

        Returns:
            Tuple[Any, ...]: A tuple of the ComplexJoinRule's attribute values.
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


class ComplexJoinDaemon:
    def __init__(
        self,
        on: Union[str, ComplexJoinRule, Sequence[Union[str, ComplexJoinRule]]] = None,
        suffixes: Union[str, Sequence[str]] = None,
        select_cols: Union[str, Sequence[str]] = None,
    ) -> None:
        self._on = self._prep_ons(on)
        self._suffixes = iterutils.tuplify(suffixes) if suffixes else None
        self._select_cols = iterutils.tuplify(select_cols) if select_cols else None
        self._plan = self._build_plan(self._on)

    def execute(self, *frames: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
        chunks, remainder = self._chunk_dataframes(self._plan, *frames)
        results = []
        p_cols = set(frames[0].columns)
        for cjr in chunks:
            p_frame = cjr.chunks[0]
            o_frames = cjr.chunks[1:]
            for i, other in enumerate(o_frames):
                rsuffix = self._suffixes[i] if self._suffixes else "_s"
                if not other.empty:
                    o_cols = set(other.columns)
                    other["merged_on"] = ",".join(cjr.on)
                    other = (
                        other[
                            {
                                *cjr.on,
                                *o_cols.intersection(set(self._select_cols)),
                                "merged_on",
                            }
                        ]
                        if self._select_cols
                        else other
                    )
                    if cjr.thresholds:
                        p_frame = self.do_inexact(
                            p_frame, other, cjr.on, cjr.thresholds, cjr.block, rsuffix
                        )
                    else:
                        p_frame = self.do_exact(p_frame, other, cjr.on, rsuffix)
            results.append(p_frame)
        result_df = pd.concat(results)
        unmatched = self._ensure_dataframe(result_df[result_df["merged_on"].isna()])
        unmatched = unmatched[p_cols]  # type: ignore
        matched = self._ensure_dataframe(result_df[~result_df["merged_on"].isna()])
        unmatched = pd.concat([unmatched, remainder])
        return matched, unmatched

    @staticmethod
    def do_exact(
        df1: pd.DataFrame, df2: pd.DataFrame, on: Tuple[str, ...], rsuffix: str = "_s"
    ) -> pd.DataFrame:
        """
        Merges two DataFrames with overlapping columns based on exact matches in
        those columns.

        Args:
            df1 (pd.DataFrame): A DataFrame.
            df2 (pd.DataFrame): A DataFrame containing columns shared with df1.
            on (Tuple[str, ...]): Columns shared by df1 and df2, which will be
                used to left join rows from df2 onto exact matches in df1
            rsuffix (str, optional): An optional suffix to use for overlapping
                columns outside the on columns. Will only be applied to df2
                columns. Defaults to "_s".

        Returns:
            pd.DataFrame: A DataFrame containing all the rows in df1, joined
                with any matched rows from df2.
        """
        return df1.merge(df2, "left", on=on, suffixes=("", rsuffix))

    @staticmethod
    def do_inexact(
        df1: pd.DataFrame,
        df2: pd.DataFrame,
        on: Tuple[str, ...],
        thresholds: Tuple[float, ...],
        block: Tuple[str, ...] = None,
        rsuffix: str = "_s",
    ) -> pd.DataFrame:
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
        # The recordlinkage library is currently passing an argument to the
        # underlying jellyfish library that jellyfish is going to deprecate
        # eventually. Nothing to do about that so just suppress it:
        warnings.filterwarnings(
            "ignore", message="the name 'jaro_winkler'", category=DeprecationWarning
        )
        idxr = link.Index()
        idxr.block(block) if block is not None else idxr.full()
        candidate_links = idxr.index(df1, df2)
        compare = link.Compare()
        # Create copies since contents of the Dataframe need to be changed.
        frames = (df1.copy(), df2.copy())
        for i, o in enumerate(on):
            compare.string(o, o, method="jarowinkler", threshold=thresholds[i])
            # Any columns containing strings should be lowercase to improve
            # matching:
            for f in frames:
                if f.dtypes[o] == "O":
                    # Pyright can't tell that DataFrame.copy() returns DataFrame.
                    f[o] = f[o].astype(str).str.lower()  # type: ignore
        features = compare.compute(candidate_links, *frames)
        matches = features[features.sum(axis=1) == len(on)].reset_index()
        a = matches.join(df1, on="level_0", how="outer", rsuffix="")
        b = a.join(df2, on="level_1", how="left", rsuffix=rsuffix)
        drop_cols = ["level_0", "level_1", *[i for i in range(len(on))]]
        b.drop(columns=drop_cols, inplace=True)
        return b

    @staticmethod
    def _ensure_dataframe(x: Union[pd.DataFrame, pd.Series]) -> pd.DataFrame:
        if isinstance(x, pd.Series):
            if isinstance(x.index, pd.RangeIndex):
                columns = ["_x"]
                s = x
            else:
                columns = x.index
                s = [x]
            return pd.DataFrame(s, columns=columns)
        else:
            return x

    @classmethod
    def _chunk_dataframes(
        cls, plan: Tuple[ComplexJoinRule, ...], *frames: pd.DataFrame
    ) -> Tuple[Tuple[ComplexJoinRule, ...], pd.DataFrame]:
        df1 = frames[0]
        for df in frames:
            for p in plan:
                match, result = cls._slice_dataframe(df, p.conditions)
                p.append(match)
                if result:
                    df.drop(match.index, inplace=True)
        return plan, df1

    @staticmethod
    def _slice_dataframe(
        df: pd.DataFrame, conditions: Dict[Optional[str], Tuple[Optional[Any], ...]]
    ) -> Tuple[pd.DataFrame, bool]:
        """
        Takes a dictionary of conditions in the form of:
            {'column_label': tuple(of, values, to, match)
        and returns a dataframe that contains only the rows that match all the
        passed conditions.


        Args:
            df (pd.DataFrame): A DataFrame.
            conditions (Dict[str, Tuple[Any, ...]]): Paired column_labels and
                tuples of values to match against.

        Returns:
            Tuple[pd.DataFrame, bool]: A DataFrame containing only the matching
                rows and a boolean indicating whether matching rows were found.
        """
        df = pd.DataFrame(df.copy())
        row_ct = df.shape[0]
        no_conditions = True
        for k, v in conditions.items():
            if k is not None and v is not None:
                no_conditions = False
                df = pd.DataFrame(df[df[k].isin(v)])
        new_ct = df.shape[0]
        result = True if (row_ct >= new_ct != 0 or no_conditions) else False
        return df, result

    @staticmethod
    def _build_plan(
        on: Tuple[Union[str, ComplexJoinRule, Tuple[str, ...], None], ...]
    ) -> Tuple[ComplexJoinRule, ...]:
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
            if isinstance(o, ComplexJoinRule):
                complex_ons.append(o)
            elif isinstance(o, str):
                simple_ons.append(o)
        if len(simple_ons) > 0:
            complex_ons.append(ComplexJoinRule(*simple_ons))
        return tuple(complex_ons)

    @staticmethod
    def _prep_ons(
        ons: Union[str, ComplexJoinRule, Sequence[Union[str, ComplexJoinRule]]] = None
    ) -> Tuple[Union[str, ComplexJoinRule, Tuple[str, ...], None], ...]:
        """
        Ensures the passed ons are valid for use in build_plan.

        Args:
            ons (Union[str, Sequence[str, ...]], optional): ons to prep. Default
                is None.

        Returns:
            Union[Tuple[Tuple[str, ...]], Tuple[str, ...], Tuple[None]]: Prepped
                ons.
        """
        if isinstance(ons, tuple):
            result = (ons,)
        elif ons is None:
            result = (ons,)
        elif isinstance(ons, (str, ComplexJoinRule)):
            result = tuple([ons])
        else:
            result = iterutils.tuplify(ons)
        return result

    @staticmethod
    def _prep_suffixes(
        frame_ct: int, suffixes: Union[str, Tuple[str, ...]] = None
    ) -> Tuple[str, ...]:
        """
        Ensures the passed suffixes are valid for use.

        Args:
            frame_ct (int): The number of other frames that ComplexJoinDaemon
                will process.
            suffixes (Union[str, Tuple[str, ...]], optional): The suffixes to be
                used. Defaults to None.

        Raises:
            ValueError: If passed a tuple of suffixes that has a length that
                doesn't match frame_ct.

        Returns:
            Tuple[str, ...]: A tuple of validated suffixes.
        """
        if suffixes is None:
            suffixes = tuple(["_" + a for a in iterutils.gen_alpha_keys(frame_ct)])
        else:
            suffixes = iterutils.tuplify(suffixes)
        if len(suffixes) != frame_ct:
            raise ValueError(
                f"Length of suffixes must be equal to the number of other "
                f"frames. Suffix len={len(suffixes)}, suffixes={suffixes}"
            )
        return suffixes


def accrete(
    df: pd.DataFrame,
    accrete_group_by: List[str],
    accretion_cols: Union[str, Tuple[str, ...]],
    accretion_sep: str = " ",
) -> pd.DataFrame:
    """
    Groups the dataframe by the passed group_by values and then concatenates
    values in the accretion columns with the passsed seperator between them.

    Args:
        df (pd.DataFrame): A DataFrame
        accrete_group_by (List[str]): A list of labels, columns to group df by.
        accretion_cols (Union[str, Tuple[str, ...]]): The columns you want to
            concatenate within each group resulting from accrete_group_by.
        accretion_sep (str, optional): The value to separate the concatenated
            values. Defaults to " ".

    Returns:
        pd.DataFrame: The modified DataFrame.
    """
    accretion_cols = iterutils.tuplify(accretion_cols)
    for c in accretion_cols:
        df[c] = df[c].fillna("")
        df[c] = df[c].astype(str)
        result = df.groupby(accrete_group_by)[c].apply(accretion_sep.join).reset_index()
        df = df.merge(result, on=accrete_group_by, suffixes=("", "_x"))
        cx = c + "_x"
        df[c] = df[cx]
        df.drop(columns=cx, inplace=True)
        df[c] = df[c].str.strip()
        df[c] = df[c].apply(
            lambda x: x if len(x) > 0 and x[-1] != accretion_sep else x[:-1]
        )
        df[c] = df[c].replace("", nan)
    return df


def complex_join(
    *frames: pd.DataFrame,
    on: Union[str, Sequence[str]] = None,
    select_cols: Union[str, Sequence[str]] = None,
    suffixes: Union[str, Sequence[str]] = None,
) -> pd.DataFrame:
    d = ComplexJoinDaemon(on, suffixes)
    sel_cols = iterutils.tuplify(select_cols)


def multiapply(
    df: pd.DataFrame,
    *columns: str,
    func: Callable[[Any], Any] = None,
    **column_func_pairs: Callable[[Any], Any],
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
        pd.DataFrame: The modified DataFrame.
    """
    col_func_map = column_func_pairs
    if func is not None:
        col_func_map = {**column_func_pairs, **{c: func for c in columns}}
    for column, f in col_func_map.items():
        df[column] = df[column].apply(f)
    return df
