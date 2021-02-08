from typing import Optional, Any, Union, Collection, Callable, TypeVar
import functools
import warnings

import pandas as pd
from numpy import nan

from . import metadata
from .config import config

_TFunc = TypeVar("_TFunc", bound=Callable[..., Any])


def nullable(
    # TODO: Fix bug here that forces args passed by pandas.apply to be kwargs.
    #       The decorator is forcing the positional args to be iterables.
    func: Optional[Any] = None,
    *,
    null_return: Optional[Any] = nan
) -> Union[Any, _TFunc]:  # type: ignore
    """
    An easy way to wrap functions that need to not execute if they are used in a
    DataFrame/Series.apply call on data that contains nan values. Simply use this
    decorator and the function will return nan if passed a nan value.
    -
    Args:
        func (Optional[Any], optional): A callable object Defaults to None.
        null_return (Optional[Any], optional): Value to return if the wrapped func
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
            # Need to avoid an error when passing the various pandas nan
            # detection functions, which cannot handle any kind of list-like:
            if isinstance(arg1, Collection) or pd.notna(arg1):
                return _func(*args, **kwargs)
            else:
                return null_return

        return wrapper_nullable

    if not isinstance(func, Callable):
        return decorator_nullable
    else:
        return decorator_nullable(func)


def resolution(
    func: Callable,
) -> Union[Any, _TFunc]: # type: ignore
    @functools.wraps(func)
    def wrapper_resolution(*args, **kwargs):
        result = func(*args, **kwargs)
        if isinstance(result, tuple) and config.env == "prod":
            result1 = result[1]
            result = result[0]
            if isinstance(result1, dict):
                metadata.METADATA.collect(func.__name__, **result1)
        return result
            
    return wrapper_resolution
