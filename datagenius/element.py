import collections.abc as abc


import datagenius.util as u


class ZeroNumeric:
    # TODO: Make this more powerful by storing the zeros separately
    #       and the numeric portion as a numeric value, so that
    #       mathematical operations can be carried out on it without
    #       sacrificing the initial zeros.
    """
    Numeric strings that need to have one or more zeros on their left
    side.
    """
    def __init__(self, value):
        """

        Args:
            value: A string that could be converted to a numeric value.
        """
        if not u.isnumericplus(value):
            raise ValueError(f'ZeroNumeric must be a numeric string or '
                             f'a numeric value. Invalid value={value}')
        self._value = str(value)

    def pad(self, length: int) -> str:
        """
        Returns the value of the ZeroNumeric but with additional zeros
        on the left side to ensure the length of the ZeroNumeric is
        equal to length.

        Args:
            length: An integer, the desired character length of the
                padded ZeroNumeric

        Returns: The value of the ZeroNumeric with as many zeros on the
            left as are necessary for len(ZeroNumeric) == length.

        """
        return '0' * max(length - len(self), 0) + self._value

    def __eq__(self, other):
        return self._value == other

    def __len__(self):
        return len(self._value)

    def __ne__(self, other):
        return self._value != other

    def __repr__(self):
        return self._value

    def __str__(self):
        return "'" + self._value


class MatchRule(abc.MutableSequence):
    """
    A simple object used by genius.Supplement to control what rules
    it uses for creating and merging chunks of Datasets.
    """
    def __init__(self, *on, conditions: dict = None,
                 thresholds: (float, tuple) = None,
                 block: (str, tuple) = None,
                 inexact: bool = False):
        """

        Args:
            *on: An arbitrary list of
            conditions:
            thresholds:
            block:
        """
        self.on: tuple = on
        c = {None: (None,)} if conditions is None else conditions
        for k, v in c.items():
            c[k] = u.tuplify(v)
        self.conditions: dict = c
        self.thresholds: tuple = u.tuplify(thresholds)
        self.block: tuple = u.tuplify(block)
        self.inexact: bool = inexact
        if self.inexact:
            if self.thresholds is None:
                self.thresholds = tuple([.9 for _ in range(len(self.on))])
            elif len(self.thresholds) != len(self.on):
                raise ValueError(
                    f'If provided, thresholds length must match on '
                    f'length: thresholds={self.thresholds}, on={self.on}')
        self.chunks: list = []

    def insert(self, index: int, x):
        self.chunks.insert(index, x)

    def output(self, *attrs) -> tuple:
        """
        Convenience method for quickly collecting a tuple of attributes
        from MatchRule.

        Args:
            *attrs: An arbitrary number of strings, which must be
                attributes in MatchRule. If no attrs are passed, output
                will just return on and conditions attributes.

        Returns: A tuple of attribute values.

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
