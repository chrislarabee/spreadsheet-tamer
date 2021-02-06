from typing import Union, Any, List, Callable, Iterable, Type

import pandas as pd

from .type_handling import isnumericplus


def broadcast_affix(x: Iterable[str], affix: str, pos: int = -1) -> List[str]:
    """
    Appends or prepends the passed affix to every value in the passed list.
    -
    Args:
        x (Iterable[str]): Any iterable of strings.
        affix (str): The string to add to each string in the iterable.
        pos (int, optional): 0 to prepend the affix, -1 to append the affix.
            Defaults to -1.
    -
    Returns:
        List[str]: x as a list with affix added to each element.
    """
    prefix = affix if pos == 0 else ""
    suffix = affix if pos == -1 else ""
    return [f"{prefix}{i}{suffix}" for i in list(x)]


def broadcast_type(
    x: Iterable[Any], type_func: Union[Callable[[Any], Any], Type]
) -> List[Any]:
    """
    Applies the passed type conversion function to each element in the passed
    list. Note that if you pass isnumeric plus broadcast_type has special
    functionality and will use the results of isnumericplus to determine what
    type to convert numeric strings to.
    -
    Args:
        x (Iterable[Any]): Any iterable to broadcast types over.
        type_func (Union[Callable[[Any], Any], Type]): A python type (int, float,
            etc) or any Callable that takes a single argument and returns a
            single object.
    -
    Returns:
        List[Any]: x as a list with type_func applied to each element.
    """
    result = []
    for val in x:
        if type_func.__name__ == "isnumericplus":
            _, t = isnumericplus(val, return_type=True)
        else:
            t = type_func
        result.append(t(val))
    return result
