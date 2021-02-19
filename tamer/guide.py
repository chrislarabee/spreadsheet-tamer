from __future__ import annotations

from typing import Any, Optional
from pathlib import Path


class Rule:
    def __init__(
        self,
        target: Any,
        into: Optional[Any] = None,
        redistribute: Optional[str] = None,
        cast: Optional[Any] = None,
        target_pattern: bool = False,
        into_pattern: bool = False,
    ) -> None:
        self._label = None
        self._pattern_map = None
        self._value_map = None


class Guide:
    def __init__(self, **rules: Rule) -> None:
        pass

    def to_yaml(self):
        pass

    @classmethod
    def from_yaml(cls, p: Path) -> Guide:
        pass
