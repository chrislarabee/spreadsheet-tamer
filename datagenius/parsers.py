import functools

import datagenius.dataset as d
import datagenius.util as u


def parser(func=None, *,
           breaks_loop=False,
           null_val=None,
           requires_header=True):
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
        return wrapper_parser
    # Allows parser to be used without arguments:
    if func is None:
        return decorator_parser
    else:
        return decorator_parser(func)


@parser(requires_header=False)
def cleanse_gaps(dset: d.Dataset, threshold: int = 1) -> list:
    """
    Uses a Dataset's loop to create a list of rows that have
    sufficient non-null values.

    Args:
        dset: A Dataset object.
        threshold: An integer, indicates the number of # of
            columns in the Dataset that can be null without
            rejecting a row. Default is at most one null value.

    Returns: A list containing only non-gap rows.

    """
    t = dset.col_ct - threshold
    return dset.loop(
        parser(lambda x: None if u.non_null_count(x) < t else x)
    )
