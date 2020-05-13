import functools

import datagenius.util as u


def parser(func=None, *,
           breaks_loop=False,
           null_val=None,
           requires_header=True,
           set_parser=False,
           takes_args=False,
           uses_cache=False):
    """
    Acts as a wrapper for other functions so that functions passed
    to Dataset.loop have all the necessary attributes for successfully
    managing the loop.

    Args:
        func: A callable object.
        breaks_loop: A boolean, indicates that when the parser
            successfully executes in a loop, the loop should break.
        null_val: Leave this as None unless you need your parser
            to return None on a successful execution.
        requires_header: A boolean, indicates whether this parser
            can be run in a Dataset that has had its header
            row built (True) or not (False).
        set_parser: A boolean, indicates whether this parser can
            be run on a full Dataset or if it's meant to be run
            potentially with other parsers on each row in turn.
        takes_args: A boolean, indicates whether this parser can
            be run with arguments beyond one positional argument.
        uses_cache: A boolean, indicates whether this parser needs
            to reference the previous result of the parser to
            execute successfully. Cannot be True if set_parser
            is also True.

    Returns: Passed func, but decorated.

    """
    # Allows parser functions to have state tracking:
    def decorator_parser(_func):
        # Allows parser to be used as a decorator.
        @functools.wraps(_func)
        def wrapper_parser(*args, **kwargs):
            return _func(*args, **kwargs)
        # Attributes of parser functions expected by other objects:
        wrapper_parser.breaks_loop = breaks_loop
        wrapper_parser.null_val = null_val
        wrapper_parser.requires_header = requires_header
        wrapper_parser.set_parser = set_parser
        wrapper_parser.takes_args = takes_args
        if set_parser and uses_cache:
            raise ValueError('set_parsers cannot use cache.')
        else:
            wrapper_parser.uses_cache = uses_cache
        wrapper_parser.is_parser = True
        return wrapper_parser
    # Allows parser to be used without arguments:
    if func is None:
        return decorator_parser
    else:
        return decorator_parser(func)


@parser(requires_header=False, set_parser=True, takes_args=True)
def cleanse_gap(x: list, threshold: int = None):
    """
    Checks a list to see if it has sufficient non-null values.

    Args:
        x: A list.
        threshold: An integer, indicates the number of # of
            values in the list that must be non-nulls. If None,
            uses the length of the list.

    Returns: The list if it contains equal to or greater non-null
        values than the threshold, otherwise None.

    """
    if threshold is None:
        w = len(x)
    else:
        w = threshold
    nn = u.non_null_count(x)
    if nn >= w:
        return x
    else:
        return None


@parser(requires_header=False, breaks_loop=True, set_parser=True)
def detect_header(x: list):
    """
    Checks a list to see if it contains only strings. If it does,
    then it could probably be a header row.

    Args:
        x: A list

    Returns: The list if it contains only non-null strings,
        otherwise None.

    """
    w = len(x)
    ts = u.true_str_count(x)
    if ts == w:
        return x
    else:
        return None


@parser(takes_args=True, uses_cache=True)
def extrapolate(x: list, indices: list, cache: (list, None) = None):
    """
    Uses the values in a cached row to fill in values in the current
    row by index. Useful when your dataset has grouped rows.

    Args:
        x: A list.
        indices: A list of integers, indices found in x.
        cache: A list, which contains values to be pulled by index
            in indices into x. If cache is None, extrapolate will
            just return a copy of x.

    Returns: x with null values overwritten with populated values
        from the cached list.

    """
    result = x.copy()
    if cache is not None:
        for i in indices:
            if result[i] in (None, ''):
                result[i] = cache[i]
    return result
