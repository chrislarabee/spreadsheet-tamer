import re
from collections import abc

from numpy import nan

import datagenius.util as u


class CleaningGuide(abc.Mapping, abc.Callable):
    """
    Convenience class for use with datagenius.clean.cleanse_typos.
    Designed to make it easier to write complex mappings between typos
    and corrected values. Any value passed to the CleaningGuide will
    be checked against the key values in the passed mapping arguments,
    and, if found in the key values, the alternative mapped value will
    be returned.
    """

    def __init__(self, *complex_maps, **simple_maps):
        """

        Args:
            *complex_maps: Arbitrary list of tuples, the first index of
                which can be a value or a tuple of values.
            **simple_maps: Arbitrary list of keyword arguments.
        """
        data = dict()

        for x in complex_maps:
            data[u.tuplify(x[0])] = x[1]
        for k, v in simple_maps.items():
            data[u.tuplify(k)] = v
        self._data = data

    @classmethod
    def convert(cls, incoming):
        """
        Ensures incoming is a CleaningGuide object, or a dict that can
        be converted to a CleaningGuide object.

        Args:
            incoming: Any object.

        Returns: A CleaningGuide object using incoming's data.

        """
        if isinstance(incoming, CleaningGuide):
            return incoming
        elif isinstance(incoming, dict):
            return CleaningGuide(**incoming)
        else:
            raise ValueError(
                f"Must pass a dict or CleaningGuide object. "
                f"Invalid object={incoming}, "
                f"type={type(incoming)}"
            )

    def __call__(self, check):
        """
        Compares check with the keys in self._data and returns the
        corresponding stored value if check is found in a key.

        Args:
            check: Any value.

        Returns: The passed check object, or its replacement if a match
            is found.

        """
        for k, v in self.items():
            if check in k:
                return v
        return check

    def __getitem__(self, item):
        return self._data[item]

    def __iter__(self):
        return self._data.__iter__()

    def __len__(self):
        return len(self._data)


class SupplementGuide(abc.MutableSequence):
    """
    A simple object used by supplement functions below to control what
    rules they use for creating and merging chunks of DataFrames.
    """

    def __init__(
        self,
        *on,
        conditions: dict = None,
        thresholds: (float, tuple) = None,
        block: (str, tuple) = None,
        inexact: bool = False,
    ):
        """

        Args:
            *on: An arbitrary list of strings, names of columns in the
                target DataFrame.
            conditions: A dictionary of conditions which rows in the
                target DataFrame must meet in order to qualify for this
                Supplement guide's instructions. Keys are column names
                and values are the value(s) in that column that qualify.
            thresholds: A float or tuple of floats of the same length
                as on. Used only if inexact is True, each threshold
                will be used with the on at the same index and matches
                in that column must equal or exceed the threshold to
                qualify as a match.
            block: A string or tuple of strings, column names in the
                target DataFrame. Use this if you're lucky enough to
                have data that you can match partially exactly on and
                just need inexact matches within that set of exact
                matches.
            inexact: A boolean, indicates whether this SupplementGuide
                represents exact or inexact match guidelines.
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
                self.thresholds = tuple([0.9 for _ in range(len(self.on))])
            elif len(self.thresholds) != len(self.on):
                raise ValueError(
                    f"If provided, thresholds length must match on "
                    f"length: thresholds={self.thresholds}, on={self.on}"
                )
        self.chunks: list = []

    def insert(self, index: int, x):
        self.chunks.insert(index, x)

    def output(self, *attrs) -> tuple:
        """
        Convenience method for quickly collecting a tuple of attributes
        from SupplementGuide.

        Args:
            *attrs: An arbitrary number of strings, which must be
                attributes in SupplementGuide. If no attrs are passed, output
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


class RedistributionGuide(abc.Callable):
    """
    Used by lib.clean.redistribute to guide its redistribution of some
    values in one column to a different column.
    """

    def __init__(self, *patterns, destination: str, mode: str = "fillna"):
        """

        Args:
            *patterns: An arbitrary list of strings, can be regex
                patterns.
            destination: The destination column to send qualifying
                values to.
            mode: A string, tells redistribute what to do with
                qualifying values when the destination column already
                has a value present. Available modes are overwrite
                (self-explanatory), append (add qualifying values after
                a space to any existing values in destination), and
                fillna (place qualifying values in destination only if
                destination is nan, remove qualifying values otherwise).
        """
        self.patterns: tuple = patterns
        self.destination: str = destination
        valid_modes = ("overwrite", "append", "fillna")
        if mode in valid_modes:
            self.mode: str = mode
        else:
            raise ValueError(
                f"Parameter mode must be one of {valid_modes}, passed " f"mode = {mode}"
            )

    def __call__(self, check):
        """
        Compares check with the patterns in self.patterns and returns
        the check if it matches, otherwise returns a numpy nan.

        Args:
            check: Any value.

        Returns: The passed check object, or nan if no match was found.
        """
        for p in self.patterns:
            if re.search(p, str(check)) is not None:
                return check
        return nan
