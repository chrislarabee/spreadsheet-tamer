from __future__ import annotations

import re
import operator as o
from typing import Union, Tuple, Any, Callable

import pandas as pd

from .. import type_handling as tc
from .. import decorators


class ZeroNumeric:
    def __init__(self, value: Union[str, tc.Numeric]):
        """
        Numeric values that need to have one or more zeros on their left side,
        thus making them somewhat string-like.
        -
        Args:
            value (Union[int, float, str]): The string, integer, or float to
                reinterpret as a ZeroNumeric.
        -
        Raises:
            ValueError: Will raise a ValueError if passed a float NaN.
            ValueError: Will raise a ValueError if passed a string that can't be
                interpreted as a numeric or any other non-numeric value.
        """
        self._zeros = ""
        if pd.isna(value):
            raise ValueError("Cannot convert float NaN to ZeroNumeric")
        elif isinstance(value, (float, int)):
            self._value = str(value)
            self._zeros = ""
            self._numeric = value
        else:
            value = str(value)
            value = re.sub(r"'+", "", value)
            if tc.isnumericplus(value):
                self._value = value
                self._zeros, self._numeric = self.split_zeros(value)
            else:
                raise ValueError(
                    f"ZeroNumeric must be a numeric string or value. "
                    f"Invalid value={value}"
                )

    @property
    def zeros(self) -> str:
        return self._zeros

    @property
    def value(self) -> str:
        return self._value

    @property
    def numeric(self) -> tc.Numeric:
        return self._numeric

    def pad(self, length: int = 0) -> ZeroNumeric:
        """
        Returns the value of the ZeroNumeric but with additional zeros on the
        left side to ensure the length of the ZeroNumeric is equal to length.
        -
        Args:
            length (int, optional): The desired character length of the padded
                ZeroNumeric.
        -
        Returns:
            ZeroNumeric: A ZeroNumeric object with as many zeros on the left as
            are necessary for len(self) == length.
        """
        z = max(length - len(self), 0)
        return ZeroNumeric("0" * z + str(self._numeric))

    @staticmethod
    def split_zeros(value: str) -> Tuple[str, Union[int, float]]:
        """
        Breaks a zero-initial numeric string into two pieces, the leading zeros,
        and the remaining numerals.
        -
        Args:
            value: A numeric value stored as a string.
        -
        Returns:
            Tuple[str, Union[int, float]]: A tuple of zeros stored as string, and
                an integer or float value representing the remainder of the
                numeric value.
        """
        pieces = re.findall(r"(^0*)([1-9]+\d*\.*\d*)", value)[0]
        _, conv_type = tc.isnumericplus(pieces[1], True)
        return pieces[0], tc.convertplus(pieces[1], conv_type)

    @staticmethod
    @decorators.nullable
    def zn_float(zn: ZeroNumeric) -> ZeroNumeric:
        """
        Version of to_float usable with pandas apply.
        -
        Args:
            zn: A ZeroNumeric object.
        -
        Returns:
            ZeroNumeric: A ZeroNumeric object with the numeric portion as a float.
        """
        return ZeroNumeric(zn.zeros + str(float(zn.numeric)))

    @staticmethod
    @decorators.nullable
    def zn_int(zn: ZeroNumeric) -> ZeroNumeric:
        """
        Version of to_int usable with pandas apply.
        -
        Args:
            zn: A ZeroNumeric object.
        -
        Returns:
            ZeroNumeric: A ZeroNumeric object with the numeric portion as an
                integer.
        """
        return ZeroNumeric(zn.zeros + str(int(zn.numeric)))

    def to_float(self) -> ZeroNumeric:
        """
        Converts the numeric portion of the ZeroNumeric to a float.
        -
        Returns:
            ZeroNumeric: A new ZeroNumeric with the same zeros and the numeric
                portion in float format.
        """
        return self.zn_float(self)

    def to_int(self) -> ZeroNumeric:
        """
        Converts the numeric portion of the ZeroNumeric to a float.
        -
        Returns:
            ZeroNumeric: A new ZeroNumeric with the same zeros and the numeric
                portion in int format.
        """
        return self.zn_int(self)

    def _do_op(self, op: Callable[[Any, Any], Any], other) -> Any:
        """
        Runs a python operation on self._numeric or self._value if other is a
        string.
        -
        Args:
            op: A python operator object.
            other: An object.

        Returns: The result of the operation on self._value if other is
            a string, or self._numeric.

        """
        if isinstance(other, str):
            return op(self._value, other)
        else:
            return op(self._numeric, other)

    def _mod(self, new_val: Union[int, float]) -> Union[ZeroNumeric, int]:
        """
        Used by the operations below.

        Args:
            new_val (Union[int, float]): [description]
        -
        Returns:
            Union[ZeroNumeric, int]: A new ZeroNumeric object with this
                ZeroNumeric's zeros replaced by new_val. Or 0 if new_val is 0.
        """
        if new_val == 0:
            return 0
        else:
            return ZeroNumeric(self._zeros + str(new_val))

    def __add__(self, other):
        return self._mod(self._do_op(o.add, other))

    def __eq__(self, other):
        if isinstance(other, str):
            return self._value == other
        else:
            return self._numeric == other

    def __gt__(self, other):
        return self._numeric > other

    def __ge__(self, other):
        return self._numeric >= other

    def __le__(self, other):
        return self._numeric <= other

    def __len__(self):
        return len(self._value)

    def __lt__(self, other):
        return self._numeric < other

    def __mod__(self, other):
        return self._mod(self._do_op(o.mod, other))

    def __mul__(self, other):
        return self._mod(self._do_op(o.mul, other))

    def __ne__(self, other):
        if isinstance(other, str):
            return self._value != other
        else:
            return self._numeric != other

    def __repr__(self):
        return self._value

    def __str__(self):
        if pd.isna(self._value):
            return str(self._value)
        else:
            return "'" + self._value

    def __sub__(self, other):
        return self._mod(self._do_op(o.sub, other))

    def __truediv__(self, other):
        return self._mod(self._do_op(o.truediv, other))
