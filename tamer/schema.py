from __future__ import annotations

from typing import List, Any, Optional, Dict, Type, AnyStr, Tuple
from pathlib import Path
import re

import yaml
import pandas as pd


class Column:
    def __init__(
        self,
        data_type: Type,
        label: str = None,
        required: bool = False,
        unique: bool = False,
        valid_values: List[Any] = None,
        invalid_values: List[Any] = None,
        valid_patterns: List[AnyStr] = None,
        invalid_patterns: List[AnyStr] = None,
    ) -> None:
        self._label = label
        self._data_type = data_type
        self.required = required
        self.unique = unique
        if valid_values or valid_patterns:
            if invalid_values:
                invalid_values = []
            if invalid_patterns:
                invalid_patterns = []
        self.valid_values = valid_values if valid_values else []
        self.invalid_values = invalid_values if invalid_values else []
        self.valid_patterns = valid_patterns if valid_patterns else []
        self.invalid_patterns = invalid_patterns if invalid_patterns else []

    @property
    def label(self) -> Optional[str]:
        return self._label

    @label.setter
    def label(self, value: str):
        if isinstance(value, str):
            self._label = value
        else:
            raise ValueError(f"label must be a string. Passed type = {type(value)}")

    @property
    def data_type(self) -> Type:
        return self._data_type

    def evaluate(self, value: Any) -> bool:
        if (pd.isna(value) or value is None) and self.required:
            return False
        if not isinstance(value, self.data_type):
            return False
        if value in self.invalid_values:
            return False
        for v in self.invalid_patterns:
            if re.search(v, str(value)):
                return False
        if len(self.valid_values) > 0:
            for v in self.valid_values:
                if v == value:
                    return True
            else:
                return False
        if len(self.valid_patterns) > 0:
            for v in self.valid_patterns:
                if re.search(v, str(value)):
                    return True
            else:
                return False
        return True


class Schema:
    def __init__(self, **columns: Column) -> None:
        self._columns = columns
        for label, c in self._columns.items():
            c.label = label

    @property
    def columns(self) -> Dict[str, Column]:
        return self._columns

    def to_yaml(self):
        pass

    @classmethod
    def from_yaml(cls, p: Path) -> Schema:
        with open(p, "r") as r:
            raw = yaml.load(r, Loader=yaml.Loader)
        return Schema(**{label: Column(**details) for label, details in raw.items()})

    def __getitem__(self, item: str) -> Column:
        return self._columns[item]
