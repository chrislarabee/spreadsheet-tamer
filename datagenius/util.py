import re
from collections import OrderedDict

import pandas as pd


def collect_by_keys(x: (dict, OrderedDict), *keys) -> (dict, OrderedDict):
    """
    A simple function to collect an arbitrary and not-necessarily
    ordered subset of a dictionary.

    Args:
        x: A dictionary or OrderedDict.
        *keys: An arbitrary list of keys that could be found in x.

    Returns: A dictionary or OrderedDict containing only the passed
        keys. Returns an object of the same type passed.

    """
    result = type(x)()
    for k, v in x.items():
        if k in keys:
            result[k] = v
    return result


def count_nulls(x: (list, OrderedDict, dict),
                strict: bool = True) -> int:
    """
    Takes a list or dictionary and returns the number of values in it
    that are None or '' if strict is False.

    Args:
        x: A list or dictionary.
        strict: A boolean indicating whether to treat empty strings
            ('') as None.

    Returns: An integer, the count of nulls in the list.

    """
    x = list(x.values()) if isinstance(x, (OrderedDict, dict)) else x
    nulls = (None, ) if strict else (None, '')
    return sum([1 if y in nulls else 0 for y in x])


def count_true_str(x: list) -> int:
    """
    Takes a list and returns the number of values in it that are
    strings that are not ''.

    Args:
        x: A list.

    Returns: An integer, the count of non-blank strings.

    """
    return sum(
        [1 if isinstance(y, str) and y != '' else 0 for y in x]
    )


def isnumericplus(x, *options) -> (bool, tuple):
    """
    A better version of the str.isnumeric test that correctly
    identifies floats stored as strings as numeric and can convert
    them if desired.

    Args:
        x: Any object.
        options: Arbitrary number of args to alter isnumericplus'
            exact behavior. Currently in use options:
                -v: Causes isnumericplus to return the type of x.
                -convert: Causes isnumericplus to convert x to int or
                    float, if it is found to be numeric.

    Returns: A boolean.

    """
    numeric = False
    v = type(x)
    if isinstance(x, (int, float)):
        numeric = True
    elif isinstance(x, str):
        v = int if re.search(r'^\d+$', x) else v
        v = float if re.search(r'^\d+\.+\d*$', x) else v
        numeric = True if v in (int, float) else False
    result = [numeric]
    if '-v' in options:
        result.append(v)
    if '-convert' in options:
        if type(x) != v:
            if v == float:
                point_ct = len(re.search(r'\.+', x).group())
            else:
                point_ct = 0
            if point_ct > 1:
                x = re.sub(r'\.+', '.', x)
        result.append(v(x))
    return tuple(result) if len(result) > 1 else numeric


def translate_nans(data: list) -> list:
    """
    Loops a passed list and ensures numpy nans are replaced with None.

    Args:
        data: A list of lists or a list of OrderedDicts.

    Returns: The list with inner values that are nan replaced with None.

    """
    for x in data:
        if isinstance(x, OrderedDict):
            iterator = x.items()
        else:
            iterator = enumerate(x)
        for i, v in iterator:
            x[i] = None if pd.isna(v) else v
    return data


def tuplify(value, do_none: bool = False) -> tuple:
    """
    Simple function that puts the passed object value into a tuple, if
    it is not already.

    Args:
        value: Any object.
        do_none: A boolean, optionally tells tuplify to tuplify Nones.
            By default, Nones are returned untouched.

    Returns: A tuple, or None.

    """
    if (value is not None or do_none) and not isinstance(value, tuple):
        value = tuple([value])
    return value


def validate_parser(f, attr: str = 'is_parser', match=True) -> bool:
    """
    Takes an object and checks its attributes. Designed to see
    if a given function has been decorated as a parser and what '
    its parser attributes are. The additional arguments are really
    only necessary in situations where you need to check an
    parser's parser attributes without first checking if it's
    actually a parser or not.

    Args:
        f: Any object.
        attr: A string, the attribute to check against. Defaults
            to the is_parser attribute.
        match: The value to check attr against. Defaults to true.

    Returns: A boolean indicating whether the object has the passed
        attribute and if it matches the passed match.

    """
    result = False
    if hasattr(f, attr):
        if getattr(f, attr) == match:
            result = True
    return result
