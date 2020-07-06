import re
import operator as o

import pandas as pd

import datagenius.util as u


class ZeroNumeric:
    """
    Numeric strings that need to have one or more zeros on their left
    side.
    """
    @property
    def zeros(self):
        return self._zeros

    @property
    def value(self):
        return self._value

    @property
    def numeric(self):
        return self._numeric

    def __init__(self, value):
        """

        Args:
            value: A string that could be converted to a numeric value.
        """
        self._zeros: str = ''
        self._numeric: (int, float, None) = None
        if pd.isna(value):
            raise ValueError('Cannot convert float NaN to ZeroNumeric')
        elif isinstance(value, (float, int)):
            self._value = str(value)
            self._zeros = ''
            self._numeric = value
        elif isinstance(value, str):
            value = re.sub(r"'+", '', value)
            if u.isnumericplus(value):
                self._value = value
                self._zeros, self._numeric = self.split_zeros(value)
            else:
                raise ValueError(
                    f'ZeroNumeric must be a numeric string or value. '
                    f'Invalid value={value}')

    def pad(self, length: int = None):
        """
        Returns the value of the ZeroNumeric but with additional zeros
        on the left side to ensure the length of the ZeroNumeric is
        equal to length.

        Args:
            length: An integer, the desired character length of the
                padded ZeroNumeric

        Returns: A ZeroNumeric object with as many zeros on the left as
            are necessary for len(self) == length.

        """
        return ZeroNumeric(
            '0' * max(length - len(self), 0) + str(self._numeric)
        )

    @staticmethod
    def split_zeros(value: str) -> tuple:
        """
        Breaks a zero-initial numeric string into two pieces, the
        leading zeros, and the remaining numerals.

        Args:
            value: A numeric value stored as a string.

        Returns: A tuple of zeros stored as string, and an integer or
            float value representing the remainder of the numeric
            value.

        """
        pieces = re.findall(r'(^0*)([1-9]+\d*\.*\d*)', value)[0]
        return pieces[0], u.isnumericplus(
                            pieces[1], '-convert', '-no_bool')

    def do_op(self, op, other):
        """
        Runs a python operation on self._numeric.

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

    @staticmethod
    @u.nullable
    def zn_float(zn):
        """
        Usable with pandas apply.

        Args:
            zn: A ZeroNumeric object.

        Returns: A ZeroNumeric object with the numeric portion as a
            float.

        """
        return ZeroNumeric(zn.zeros + str(float(zn.numeric)))

    @staticmethod
    @u.nullable
    def zn_int(zn):
        """
        Usable with pandas apply.

        Args:
            zn: A ZeroNumeric object.

        Returns: A ZeroNumeric object with the numeric portion as an
            integer.

        """
        return ZeroNumeric(zn.zeros + str(int(zn.numeric)))

    def to_float(self):
        """
        Converts the numeric portion of the ZeroNumeric to a float.

        Returns: A new ZeroNumeric with the same zeros and the numeric
            portion in float format.

        """
        return self.zn_float(self)

    def to_int(self):
        """
        Converts the numeric portion of the ZeroNumeric to an integer.

        Returns: A new ZeroNumeric with the same zeros and the numeric
            portion in integer format.

        """
        return self.zn_int(self)

    def _mod(self, new_val: (int, float)):
        """

        Args:
            new_val: An integer or float.

        Returns: A new ZeroNumeric object with this ZeroNumeric's zeros
            followed by new_val.

        """
        if new_val is 0:
            return 0
        else:
            return ZeroNumeric(
                self._zeros + str(new_val)
            )

    def __add__(self, other):
        return self._mod(self.do_op(o.add, other))

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
        return self._mod(self.do_op(o.mod, other))

    def __mul__(self, other):
        return self._mod(self.do_op(o.mul, other))

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
        return self._mod(self.do_op(o.sub, other))

    def __truediv__(self, other):
        return self._mod(self.do_op(o.truediv, other))
