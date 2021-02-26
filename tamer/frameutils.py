from typing import Callable, Any, Optional, Union, Tuple, List, Dict, overload
from collections.abc import MutableSequence

import pandas as pd
from numpy import nan

from . import iterutils


class ComplexJoinRule(MutableSequence):
    def __init__(
        self,
        *on: str,
        conditions: dict = None,
        thresholds: Union[float, Tuple[float, ...]] = None,
        block: Union[str, Tuple[str, ...]] = None,
        inexact: bool = False,
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
            thresholds (Union[float, Tuple[float, ...]], optional): Used only if
                inexact is True, each threshold will be used with the on at the
                same index for fuzzy matching. Defaults to None.
            block (Union[str, Tuple[str, ...]], optional): Column names in the
                target DataFrame to require exact matches on. Defaults to None.
            inexact (bool, optional): Set to True if this ComplexJoinRule
                represents inexact match guidelines. Defaults to False.

        Raises:
            ValueError: If threshold values are passed and the # of thresholds
                passed does not match the # of ons patched.
        """
        self.on = on
        self._conditions = (
            {k: iterutils.tuplify(v) for k, v in conditions.items()}
            if conditions
            else {None: (None,)}
        )
        self.thresholds = iterutils.tuplify(thresholds) if thresholds else None
        self.block = iterutils.tuplify(block)
        self.inexact = inexact
        if self.inexact:
            if self.thresholds is None:
                self.thresholds = tuple([0.9 for _ in range(len(self.on))])
            elif len(self.thresholds) != len(self.on):
                raise ValueError(
                    f"If provided, thresholds length must match on length: "
                    f"thresholds={self.thresholds}, on={self.on}"
                )
        self.chunks = []

    @property
    def conditions(self) -> dict:
        return self._conditions

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
    def __init__(self) -> None:
        pass

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
    def _build_plan(on: Tuple[Any, ...]) -> Tuple[ComplexJoinRule, ...]:
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
            elif isinstance(o, tuple):
                pair = [None, None]
                for oi in o:
                    if isinstance(oi, dict):
                        pair[1] = oi  # type: ignore
                    elif isinstance(oi, (str, tuple)):
                        pair[0] = iterutils.tuplify(oi)  # type: ignore
                    else:
                        raise ValueError(
                            "tuple ons must have a dict as one of their "
                            "arguments and a str/tuple as the other Invalid "
                            f"tuple={o}"
                        )
                sg = ComplexJoinRule(*pair[0], conditions=pair[1])
                complex_ons.append(sg)
        if len(simple_ons) > 0:
            complex_ons.append(ComplexJoinRule(*simple_ons))
        return tuple(complex_ons)

    @staticmethod
    def _prep_ons(
        ons: Union[str, Tuple[str, ...]]
    ) -> Union[Tuple[Tuple[str, ...]], Tuple[str, ...]]:
        """
        Ensures the passed ons are valid for use in build_plan.

        Args:
            ons (Union[str, Tuple[str, ...]]): ons to prep.

        Returns:
            Union[Tuple[Tuple[str, ...]], Tuple[str, ...]]: Prepped ons.
        """
        if isinstance(ons, tuple):
            result = (ons,)
        elif isinstance(ons, str):
            result = tuple(list(ons))
        else:
            result = ons
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
