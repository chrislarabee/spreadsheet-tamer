from __future__ import annotations

from typing import List, Any, Optional, Dict, Type, Tuple
from pathlib import Path
import re

import yaml
import pandas as pd

from tamer.decorators import resolution
from tamer.type_handling import CollectibleMetadata


class Valid:
    def __init__(self, invalid_reason: str = None) -> None:
        self._invalid_reasons = [invalid_reason] if invalid_reason else []

    @property
    def invalid_reasons(self) -> List[str]:
        return self._invalid_reasons

    def __str__(self) -> str:
        return str(bool(self))

    def __bool__(self) -> bool:
        if self._invalid_reasons == []:
            return True
        else:
            return False

    def __repr__(self) -> str:
        return f"<Valid({bool(self)})>"

    def __add__(self, other: Valid) -> Valid:
        if isinstance(other, Valid):
            self._invalid_reasons += other.invalid_reasons
            return self
        else:
            raise TypeError(f"Can only add Valid objects to other Valid objects.")

    def __eq__(self, other: Any) -> bool:
        if isinstance(other, bool):
            return bool(self) == other
        elif isinstance(other, Valid):
            return self.invalid_reasons == other.invalid_reasons
        else:
            return False

    def __ne__(self, other: Any) -> bool:
        if isinstance(other, bool):
            return bool(self) != other
        elif isinstance(other, Valid):
            return self.invalid_reasons != other.invalid_reasons
        else:
            return True


class Column:
    def __init__(
        self,
        data_type: Type,
        label: str = None,
        required: bool = False,
        unique: bool = False,
        valid_values: List[Any] = None,
        invalid_values: List[Any] = None,
        valid_patterns: List[str] = None,
        invalid_patterns: List[str] = None,
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

    def evaluate(self, value: Any) -> Valid:
        result = Valid()
        if pd.isna(value) or value is None:
            if self.required:
                return Valid(f"Column {self._label} is required")
            else:
                return result
        elif not isinstance(value, self.data_type):
            return Valid(
                f"Column {self._label} value is not data type {self.data_type}"
            )
        if value in self.invalid_values:
            return Valid(f"<{value}> is not a valid value for Column {self._label}")
        for v in self.invalid_patterns:
            if re.search(str(v), str(value)):
                return Valid(
                    f"<{value}> matches invalid pattern <{v}> for Column {self._label}"
                )
        if len(self.valid_values) > 0:
            for v in self.valid_values:
                if v == value:
                    return result
            else:
                result = Valid(
                    f"<{value}> is not a valid value for Column {self._label}"
                )
        if len(self.valid_patterns) > 0:
            for v in self.valid_patterns:
                if re.search(str(v), str(value)):
                    return Valid()
            else:
                result = Valid(
                    f"<{value}> does not match valid patterns for Column {self._label}"
                )
        return result


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

    def validate(self, df: pd.DataFrame) -> pd.DataFrame:
        valids = pd.Series([Valid() for _ in range(len(df))])
        for c in df.columns:
            if c in self._columns:
                col = self._columns[c]
                if col.unique:
                    dupes = df[c].duplicated(False)
                    if dupes.any:
                        valids += pd.Series(
                            [
                                Valid(f"Column {c} must be unique") if x else Valid()
                                for x in dupes
                            ]
                        )
                v = df[c].apply(lambda x: col.evaluate(x))
                valids += v
        df["row_valid"] = valids
        return df

    @resolution
    def enforce_schema_rules(
        self, df: pd.DataFrame
    ) -> Tuple[pd.DataFrame, CollectibleMetadata]:
        """
        Rejects all rows in the passed DataFrame that do not pass validation
        against the Schema's rules.

        Args:
            df (pd.DataFrame): A DataFrame with at least some columns that
                correspond to this Schema.

        Returns:
            Tuple[pd.DataFrame, CollectibleMetadata]: The DataFrame with any
                invalid rows dropped and a CollectibleMetadata dictionary
                containing the rejected rows.
        """
        if "row_valid" not in list(df.columns):
            df = self.validate(df)
        rejects = df[df["row_valid"] == False]
        df.drop(index=list(rejects.index), inplace=True)
        return df, dict(rejects=rejects)
