from typing import Optional, Any, Union, Collection, Callable, TypeVar
import functools

import pandas as pd
from numpy import nan

_TFunc = TypeVar("_TFunc", bound=Callable[..., Any])


def nullable(
    func: Optional[Any] = None, *, nan_return: Optional[Any] = nan
) -> Union[Any, _TFunc]:
    """
    An easy way to wrap functions that need to not execute if they are used in a
    DataFrame/Series.apply call on data that contains nan values. Simply use this
    decorator and the function will return nan if passed a nan value.
    -
    Args:
        func (Optional[Any], optional): A callable object Defaults to None.
        nan_return (Optional[Any], optional): Value to return if the wrapped func
            is passed a nan. Defaults to nan.
    -
    Returns:
        Union[Any, _TFunc]: The result of func, or nan_return if the first
            positional argument is nan.
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
