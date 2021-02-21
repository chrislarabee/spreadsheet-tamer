from __future__ import annotations

from typing import Any, Optional, Type, Tuple, Literal, List
from pathlib import Path
import re

import pandas as pd
import numpy as np

from . import type_handling as th
from . import iterutils


class Rule:
    def __init__(
        self,
        label: str,
        target: Optional[Any] = None,
        into: Optional[Any] = None,
        redistribute: Optional[str] = None,
        redistribute_mode: Literal["overwrite", "append", "fillna"] = "fillna",
        cast: Optional[Type[Any]] = None,
        is_pattern: Optional[Literal["target", "into"]] = None,
    ) -> None:
        self._label = label
        is_pattern = "target" if not target else is_pattern
        target = target if target else r".*"
        self._target = iterutils.tuplify(target)
        self._into = into
        self._redistribute = redistribute
        self._redis_mode = redistribute_mode
        self._cast = cast
        self._target_pattern = True if is_pattern in ("target", "into") else False
        self._into_pattern = True if is_pattern == "into" else False
        if self._target_pattern and self._into_pattern and self._into:
            self._validate_target_into_patterns(self._target, self._into)

    @property
    def is_redistribute_rule(self) -> bool:
        return True if self._redistribute else False

    def apply(self, s: pd.Series) -> pd.Series:
        result = pd.Series(s.copy())
        if self._into:
            result = self.map_transform_values(result)
        if self._cast:
            result = self.cast_values(result)
        return result

    def map_transform_values(self, s: pd.Series) -> pd.Series:
        if self._target_pattern:
            if self._into_pattern:
                result = s
                for p in self._target:
                    result = s.apply(
                        lambda x: self._into.format(*re.search(p, str(x)).groups())
                        if pd.notna(x) and re.search(p, str(x))
                        else x
                    )
            else:
                result = s.apply(
                    lambda x: self._into
                    if pd.notna(x)
                    and pd.Series(
                        [re.search(p, str(x)) is not None for p in self._target]
                    ).any()
                    else x
                )
        else:
            result = s.apply(lambda x: self._into if x in self._target else x)
        return pd.Series(result)

    def cast_values(self, s: pd.Series) -> pd.Series:
        if self._target_pattern:
            result = s
            for p in self._target:
                result = s.apply(
                    lambda x: th.convertplus(x, self._cast)
                    if re.search(p, str(x))
                    else x
                )
        else:
            result = s.apply(
                lambda x: th.convertplus(x, self._cast) if x in self._target else x
            )
        return pd.Series(result)

    def get_redistribution_values(self, s: pd.Series) -> pd.Series:
        if self._target_pattern:
            result = s.apply(
                lambda x: x
                if pd.notna(x)
                and pd.Series(
                    [re.search(p, str(x)) is not None for p in self._target]
                ).any()
                else np.nan
            )
        else:
            result = s.apply(lambda x: x if x in self._target else np.nan)
        return pd.Series(result)

    @classmethod
    def _validate_target_into_patterns(cls, target_pat: Tuple[str, ...], into_pat: str):
        for tp in target_pat:
            if cls._count_char(tp, r"\(") != cls._count_char(into_pat, r"\{"):
                raise ValueError(
                    "All target_patterns must have a # of regex groups () equal "
                    "to the # of {} in into. "
                    f"{tp} does not match {into_pat}"
                )

    @staticmethod
    def _count_char(s: str, char: str) -> int:
        m = re.findall(re.compile(char), s)
        if m:
            return len(m)
        else:
            return 0


class Guide:
    def __init__(self, **rules: List[Rule]) -> None:
        self._rules = rules

    def resolve_rules(self, df: pd.DataFrame) -> pd.DataFrame:
        for label, rules in self._rules.items():
            result = pd.Series(df[label].copy())
            for rule in rules:
                result = rule.apply(result)
                if rule.is_redistribute_rule:
                    redis_vals = rule.get_redistribution_values(result)

    @staticmethod
    def _redistribute(
        s: pd.Series,
        df: pd.DataFrame,
        label: str,
        mode: Literal["overwrite", "append", "fillna"],
    ) -> pd.DataFrame:
        if mode == "overwrite":
            df[label] = s.fillna(df[label])
        elif mode == "append":
            df[label] = df[label].apply(th.convertplus, target_type=str)
            s = pd.Series(s.apply(th.convertplus, target_type=str))
            spaces = s.notna().replace([True, False], [" ", ""])
            df[label] = df[label] + spaces + s.fillna("")
            df[label] = df[label].fillna(s)
        elif mode == "fillna":
            df[label] = df[label].fillna(s)
        else:
            raise ValueError(
                f"mode must be one of ['overwrite', 'append', 'fillna']. Passed {mode}."
            )
        # pandas-stubs can't handle the result of series[].
        df.loc[s[s.notna()].index, label] = np.nan  # type: ignore
        return df

    def to_yaml(self):
        pass

    @classmethod
    def from_yaml(cls, p: Path) -> Guide:
        pass
