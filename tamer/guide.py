from __future__ import annotations

from typing import Any, Optional, Type, Tuple, Literal
from pathlib import Path
import re

import pandas as pd

from . import type_handling as th
from . import iterutils


class Rule:
    def __init__(
        self,
        label: str,
        target: Any,
        into: Optional[Any] = None,
        redistribute: Optional[str] = None,
        cast: Optional[Type[Any]] = None,
        is_pattern: Optional[Literal["target", "into"]] = None,
    ) -> None:
        self._label = label
        self._target = iterutils.tuplify(target)
        self._into = into
        self._redistribute = redistribute
        self._cast = cast
        self._target_pattern = True if is_pattern in ("target", "into") else False
        self._into_pattern = True if is_pattern == "into" else False
        if self._target_pattern and self._into_pattern and self._into:
            self._validate_target_into_patterns(self._target, self._into)

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
                    lambda x: th.convertplus(x, self._cast) if re.search(p, str(x)) else x
                )
        else:
            result = s.apply(
                lambda x: th.convertplus(x, self._cast) if x in self._target else x
            )
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
    def __init__(self, **rules: Rule) -> None:
        pass

    def to_yaml(self):
        pass

    @classmethod
    def from_yaml(cls, p: Path) -> Guide:
        pass
