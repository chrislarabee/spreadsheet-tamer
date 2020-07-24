import functools
import inspect
import re
import string
from collections import OrderedDict
from typing import Callable, Sequence, Mapping, Collection, Iterable

import pandas as pd
from numpy import nan

import datagenius.element as e
from datagenius.tms_registry import TMS


def transmutation(func=None, *, stage: str = None, priority: int = 10):
    """
    Custom functions written for use by genius pipeline stages can be
    decorated as transmutations in order to better organize information
    about their activity.

    Args:
        func: A callable object.
        stage: A string indicating the name of the stage this
            transmutation takes place in. Results of its activity will
            be placed in the same attribute on a GeniusMetadata object.
        priority: An integer indicating how early this transmutation
            should be run when grouped into a list of other
            transmutations. Higher priority transmutations will be run
            earlier.

    Returns: The passed function once decorated.

    """
    stage = '_no_stage' if stage is None else stage

    # Allows transmutation functions to have special attributes:
    def decorator_transmutation(_func):

        # Allows transmutation to be used as a decorator:
        @functools.wraps(_func)
        def wrapper_transmutation(*args, **kwargs):
            return _func(*args, **kwargs)

        # Decorated functions cannot be inspected to get args, so must
        # inspect now:
        wrapper_transmutation.args = inspect.getfullargspec(_func).args

        # Attributes of transmutation functions expected by other
        # objects:
        wrapper_transmutation.stage = (re.sub(r' +', '_', stage).lower())
        wrapper_transmutation.priority = priority

        # Registers the transmutation in the tms_registry.
        if stage not in TMS.keys():
            TMS[stage] = []
        TMS[stage].append(wrapper_transmutation)

        return wrapper_transmutation

    if not isinstance(func, Callable):
        return decorator_transmutation
    else:
        return decorator_transmutation(func)


def nullable(func: Callable = None, *, nan_return=nan):
    """
    An easy way to wrap functions that need to not execute if they are
    used in a DataFrame/Series.apply call on data that contains nan
    values. Simply use this decorator and the function will simply
    return nan if passed a nan value.

    Args:
        func: A callable object.
        nan_return: The value to return if func is passed a nan value
            as its first argument. Defaults to nan.

    Returns: The result of func, or nan if the first positional
        argument is nan.

    """
    # Allows nullable functions to take arguments:
    def decorator_nullable(_func):
        # Allows nullable to be used as a decorator:
        @functools.wraps(_func)
        def wrapper_nullable(*args, **kwargs):
            arg1 = args[0]
            # Need to avoid an error when passing the various pandas
            # nan detection functions, which cannot handle any kind of
            # list-like:
            if isinstance(arg1, Collection) or pd.notna(arg1):
                return _func(*args, **kwargs)
            else:
                return nan_return

        return wrapper_nullable

    if not isinstance(func, Callable):
        return decorator_nullable
    else:
        return decorator_nullable(func)


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
    func_args = getattr(func, 'args', None)
    if func_args is None:
        func_args = inspect.getfullargspec(func).args
    if suppress:
        suppress = (
            [suppress] if not isinstance(suppress, list) else suppress)
        for s in suppress:
            if s in func_args:
                func_args.remove(s)
    return {k: kwargs.get(k) for k in func_args}


def broadcast_suffix(
        x: (list, tuple, pd.Series, pd.Index),
        suffix: str) -> list:
    """
    Appends the passed suffix to every value in the passed list.

    Args:
        x: A list of strings.
        suffix: The string to append to each value in x.

    Returns: x, with suffix appended to each value.

    """
    return [i + suffix for i in list(x)]


def broadcast_type(x: (list, tuple, pd.Series), type_func: Callable):
    """
    Applies the passed type conversion function to each element in the
    passed list. Note that if you pass isnumeric plus broadcast_type
    has special functionality and will use the results of isnumericplus
    to determine what type to convert numeric strings to.

    Args:
        x: A list or Sequence.
        type_func: A callable function to convert objects in the list.

    Returns: The list or Sequence with each element replaced by the
        result of calling type_func on it.

    """
    for i, val in enumerate(x):
        if type_func == isnumericplus:
            b, t = isnumericplus(val, '-v')
        else:
            t = type_func
        x[i] = t(val)
    return x


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


def enforce_uniques(x: list) -> list:
    """
    Loops a list and appends incremental numerals to any repeated
    values.

    Args:
        x: A list.

    Returns: The list, with any repeated values converted to strings
        and appended with _X, where X is an integer.

    """
    values = dict()
    for i, val in enumerate(x):
        if val in values.keys():
            x[i] = str(val) + '_' + str(values[val])
            values[val] += 1
        else:
            values[val] = 1
    return x


def gen_alpha_keys(num: int) -> list:
    """
    Generates a set of characters from the Latin alphabet a la excel
    headers.

    Args:
        num: The desired length of the set.

    Returns: A set containing as many letters and letter combos as
        desired. Can be used to generate sets up to 676 in length.

    """
    a = string.ascii_uppercase
    result = list()
    x = num // 26
    for i in range(x + 1):
        root = a[i - 1] if i > 0 else ''
        keys = [root + a[j] for j in range(26)]
        for k in keys:
            result.append(k) if len(result) < num else None
    return result


def gen_empty_md_df(columns: Sequence, default_val=0) -> pd.DataFrame:
    """
    Generates an empty DataFrame with the passed columns and a one row
    placeholder. Used in functions that will accumulate metadata into
    an empty DataFrame.

    Args:
        columns: A Sequence of column names to use in the empty df.
        default_val: The default value to put in each column in the
            empty df.

    Returns: A DataFrame with the passed columns and a single row
        containing a zero in each of those columns.

    """
    return pd.DataFrame([[default_val for _ in columns]], columns=columns)


@nullable(nan_return='nan')
def get_class_name(obj) -> str:
    """
    Gets the name of the passed object's class, even if it doesn't
    have a __name__ attribute.

    Args:
        obj: An object.

    Returns: A string representing the name of the object's class.

    """
    t = type(obj)
    return re.findall(r"<class '(.+)'>", str(t))[0]


@nullable
def gconvert(obj, target_type):
    """
    Smart type conversion that avoids errors when converting to numeric
    from non-standard strings.

    Args:
        obj: Any object.
        target_type: The target type to convert to.

    Returns:

    """
    if (isinstance(obj, str)
            and target_type == float
            and isnumericplus(obj)):
        pts = re.search(r'\.+', obj)
        point_ct = len(pts.group()) if pts else 0
        if point_ct > 1:
            obj = re.sub(r'\.+', '.', obj)
    return target_type(obj)


@nullable
def gtype(obj):
    """
    Wrapper for type that distinguishes nan values as nan and not
    float.

    Args:
        obj: Any object.

    Returns: The type of the object, or nan if it is a numpy nan.

    """
    return type(obj)


def gwithin(within: Sequence, *values) -> bool:
    """
    A more sophisticated way to execute "x in iterable" type python
    statements. Allows searching for multiple values at once and using
    regex.

    Args:
        within: A sequence to search within.
        *values: An arbitrary list of values. Strings will be used to
            create regex-based matches against each value in within. If
            any of the values are found in within, gwithin will return
            True.

    Returns: A boolean indicating whether any of the objects in values
        are contained in within.

    """
    result = False
    for v in values:
        if isinstance(v, str):
            for w in within:
                if re.search(v, str(w)) is not None:
                    result = True
                    break
        else:
            if v in within:
                result = True
                break
    return result


def isnumericplus(x, *options) -> (bool, tuple):
    """
    A better version of the str.isnumeric test that correctly
    identifies floats stored as strings as numeric.

    Args:
        x: Any object.
        options: Arbitrary number of args to alter isnumericplus'
            exact behavior. Currently in use options:
                -v: Causes isnumericplus to return the type of x.

    Returns: A boolean or tuple if options were passed.

    """
    numeric = False
    v = type(x)
    if isinstance(x, (int, float)):
        numeric = True
    elif isinstance(x, str):
        v = int if re.search(r'^-*\d+$', x) else v
        v = e.ZeroNumeric if x[0] == '0' and x not in ('0', '0.00') else v
        v = float if re.search(r'^-*\d+\.+\d*$', x) else v
        numeric = True if v in (int, float, e.ZeroNumeric) else False
    result = [numeric]
    if '-v' in options:
        result.append(v)
    return tuple(result) if len(result) > 1 else result[0]


def package_rejects_metadata(df: pd.DataFrame):
    """
    Convenience function for creating a metadata dictionary containing
    rejects and counts of values in those rejects.

    Args:
        df: A DataFrame of rejected rows.

    Returns: A dictionary containing df and a single-row df containing
        counts of values in df.

    """
    return dict(
        rejects=df,
        metadata=pd.DataFrame(df.count()).T
    )


def purge_gap_rows(df: pd.DataFrame) -> pd.DataFrame:
    """
    Takes a DataFrame object and drops rows that are entirely nan.

    Args:
        df: A DataFrame.

    Returns: A DataFrame without entirely nan rows.

    """
    return df.dropna(how='all').reset_index(drop=True)


def standardize_header(header: (pd.Index, list, tuple)) -> tuple:
    """
    Takes a list-like object and ensures every element in it is
    compliant as a sqlite column name.

    Args:
        header: An Index, list, or tuple.

    Returns: A tuple of the altered header as a list and the original
        header as a list.

    """
    result = []
    for h in header:
        p = string.punctuation.replace('_', '')
        h = re.sub(re.compile(r'[' + p + ']'), '', h)
        result.append(re.sub(' +', '_', h.strip()).lower())
    if len(set(result)) < len(result):
        result = enforce_uniques(result)
    return result, list(header)


def translate_null(obj, to: (nan, None) = nan):
    """
    Checks if a passed object is a NoneType object or a numpy nan and
    then converts it to the passed

    Args:
        obj: Any object.
        to: None or numpy nan. If nan will convert Nones to nan, if
            None, it will convert nan to None.

    Returns: The object, converted to None or nan as appropriate.

    """
    if not pd.isna(to) and to is not None:
        raise ValueError(f'to must be numpy nan or None. to={to}')
    if pd.isna(obj) or obj is None:
        return to
    else:
        return obj


def tuplify(value, do_none: bool = False) -> (tuple, None):
    """
    Simple function that puts the passed object value into a tuple, if
    it is not already.

    Args:
        value: Any object.
        do_none: A boolean, optionally tells tuplify to tuplify Nones.
            By default, Nones are returned untouched.

    Returns: A tuple, or None.

    """
    if not isinstance(value, tuple) and (value is not None or do_none):
        if isinstance(value, dict):
            value = [(k, v) for k, v in value.items()]
        elif not isinstance(value, Iterable) or isinstance(value, str):
            value = [value]
        return tuple(value)
    else:
        return value


def tuplify_iterable(
        value: (Sequence, Mapping),
        do_none: bool = False) -> (Sequence, Mapping):
    """
    Convenience method to apply tuplify function to the values of an
    iterable sequence.

    Args:
        value: An iterable Sequence.
        do_none: A boolean, indicates whether None values contained
            in the Sequence should be tuplified.

    Returns: The Sequence, with values rendered into tuples.

    """
    if isinstance(value, (dict, OrderedDict)):
        iterable = value.items()
    else:
        iterable = enumerate(value)
    for k, v in iterable:
        value[k] = tuplify(v, do_none)
    return value


def validate_attr(obj, attr: str, match=None) -> bool:
    """
    Takes an object and checks its attributes. Useful in situations 
    where you want to check an object's attributes without first 
    checking if it has those attributes.

    Args:
        obj: Any object.
        attr: A string, the attribute to check against.
        match: The value to check attr against. If none, simply checks
            if the attr is present.

    Returns: A boolean indicating whether the object has the passed
        attribute and if it matches the passed match.

    """
    result = False
    if hasattr(obj, attr):
        if match is None:
            result = True
        elif getattr(obj, attr) == match:
            result = True
    return result
