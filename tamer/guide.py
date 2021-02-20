from __future__ import annotations

from typing import Any, Optional, List
from pathlib import Path
import re

import pandas as pd

from .type_handling import SSType


class Rule:
    def __init__(
        self,
        label: str,
        target: Any,
        into: Optional[Any] = None,
        redistribute: Optional[str] = None,
        cast: Optional[SSType] = None,
        target_pattern: bool = False,
        into_pattern: bool = False,
    ) -> None:
        self._label = label
        self._target = target
        self._into = into
        self._redistribute = redistribute
        self._cast = cast
        self._target_pattern = target_pattern
        self._into_pattern = into_pattern
        if self._target_pattern and self._into_pattern and self._into:
            self._validate_target_into_patterns(self._target, self._into)

    def map_transform_values(self, s: pd.Series) -> pd.Series:
        if self._target:
            if self._target_pattern:
                if self._into_pattern:
                    result = s
                    for p in self._target:
                        result = s.apply(
                            lambda x: self._into.format(*re.search(p, str(x)).groups())
                            if pd.notna(x) and re.search(p, str(x)) is not None
                            else x
                        )
                    return pd.Series(result)
                else:
                    result = s.apply(
                        lambda x: self._into
                        if pd.notna(x)
                        and pd.Series(
                            [re.search(p, str(x)) is not None for p in self._target]
                        ).any()
                        else x
                    )
                    return pd.Series(result)
            else:
                result = s.apply(lambda x: self._into if x in self._target else x)
                return pd.Series(result)
        else:
            return s

    @classmethod
    def _validate_target_into_patterns(cls, target_pat: List[str], into_pat: str):
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
