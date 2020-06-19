import functools
import inspect
import re
import string
from collections import OrderedDict
from typing import Callable

import pandas as pd


def transmutation(func=None, *, stage=None):
    """
    Custom functions written for use by genius pipeline stages can be
    decorated as transmutations in order to better organize information
    about their activity.

    Args:
        func: A callable object.
        stage: A string indicating the name of the stage this
            transmutation takes place in. Results of its activity will
            be placed in the same attribute on a GeniusMetadata object.

    Returns: The passed function once decorated.

    """
    # Allows transmutation functions to have special attributes:
    def decorator_transmutation(_func):

        # Allows transmutation to be used as a decorator:
        @functools.wraps(_func)
        def wrapper_transmutation(*args, **kwargs):
            return _func(*args, **kwargs)
        # Attributes of transmutation functions expected by other
        # objects:
        wrapper_transmutation.stage = re.sub(r' +', '_', stage).lower()
        return wrapper_transmutation

    if not isinstance(func, Callable):
        return decorator_transmutation
    else:
        return decorator_transmutation(func)


def align_args(func: Callable, kwargs: dict,
               suppress: (list, str) = None) -> dict:
    """
    Plucks only kwargs used by the passed function from the passed
    kwargs dict. Can also suppress any number of kwargs that do match,
    but which shouldn't be used.

    Args:
        func: A callable object.
        kwargs: A dictionary of kwargs, any number of which could be
            used by func.
        suppress: A list of kwargs to not pass to func, even if their
            names match.

    Returns: A dictionary containing only the kwargs key-value pairs
        that func can accept.

    """
    func_args = inspect.getfullargspec(func).args
    if suppress:
        suppress = (
            [suppress] if not isinstance(suppress, list) else suppress)
        for s in suppress:
            func_args.remove(s)
    return {k: kwargs.get(k) for k in func_args}


def clean_whitespace(x) -> list:
    """
    When passed a string, removes leading and trailing whitespace from
    it and also replaces any chains of more than one space with a
    single space.

    Args:
        x: An object.

    Returns: A tuple containing a boolean that indicates whether x was
        cleaned, and x, cleaned of whitespace if applicable.

    """
    cleaned = False
    clean_x = x
    if isinstance(clean_x, str):
        clean_x = re.sub(r' +', ' ', x.strip())
        cleaned = True if clean_x != x else False
    return [cleaned, clean_x]


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


def count_true_str(x: (list, pd.Series)) -> int:
    """
    Takes a list or pandas Series and returns the number of values in
    it that are strings that are not ''.

    Args:
        x: A list or pandas Series.

    Returns: An integer, the count of non-blank strings.

    """
    return sum(
        [1 if isinstance(y, str) and y != '' else 0 for y in x]
    )


def gen_alpha_keys(num: int) -> set:
    """
    Generates a set of characters from the Latin alphabet a la excel
    headers.

    Args:
        num: The desired length of the set.

    Returns: A set containing as many letters and letter combos as
        desired. Can be used to generate sets up to 676 in length.

    """
    a = string.ascii_uppercase
    result = set()
    x = num // 26
    for i in range(x + 1):
        root = a[i - 1] if i > 0 else ''
        keys = [root + a[j] for j in range(26)]
        for k in keys:
            result.add(k) if len(result) < num else None
    return result


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


def purge_gap_rows(df: pd.DataFrame) -> pd.DataFrame:
    """
    Takes a Dataset object and drops rows that are entirely nan.

    Args:
        df: A Dataset object.

    Returns: A Dataset without entirely nan rows.

    """
    return df.dropna(how='all').reset_index(drop=True)


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


def validate_attr(obj, attr: str, match) -> bool:
    """
    Takes an object and checks its attributes. Useful in situations 
    where you want to check an object's attributes without first 
    checking if it has those attributes.

    Args:
        obj: Any object.
        attr: A string, the attribute to check against.
        match: The value to check attr against.

    Returns: A boolean indicating whether the object has the passed
        attribute and if it matches the passed match.

    """
    result = False
    if hasattr(obj, attr):
        if getattr(obj, attr) == match:
            result = True
    return result
