import functools

from datagenius import dataset as d


def parser(func=None, *, breaks_loop=False, null_val=None):
    def decorator_parser(_func):
        @functools.wraps(_func)
        def wrapper_parser(*args, **kwargs):
            return _func(*args, **kwargs)
        wrapper_parser.breaks_loop = breaks_loop
        wrapper_parser.null_val = null_val
        return wrapper_parser
    if func is None:
        return decorator_parser
    else:
        return decorator_parser(func)


@parser
def cleanse_gaps(dset: d.Dataset, threshold: int = 1):
    t = dset.col_ct - threshold
    return dset.loop(
        parser(lambda x: None if non_null_count(x) < t else x)
    )


@parser
def non_null_count(x: list):
    return sum([0 if y in (None, '') else 1 for y in x])
