from typing import (
    Union,
    Any,
    Callable,
    Iterable,
    Type,
    Dict,
    Tuple,
    MutableSequence,
    List,
)
import re
import string

from .type_handling import isnumericplus


def broadcast_affix(x: Iterable[str], affix: str, pos: int = -1) -> Iterable[str]:
    """
    Appends or prepends the passed affix to every value in the passed list.

    Args:
        x (Iterable[str]): Any iterable of strings.
        affix (str): The string to add to each string in the iterable.
        pos (int, optional): 0 to prepend the affix, -1 to append the affix.
            Defaults to -1.

    Returns:
        Iterable[str]: x with affix added to each element.

    Raises:
        ValueError: If a non-iterable is passed for x.
    """
    if isinstance(x, Iterable):
        iter_type = type(x)
        prefix = affix if pos == 0 else ""
        suffix = affix if pos == -1 else ""
        # This must be flagged as type: ignore because pyright doesn't like the
        # variability of the flexible constructor derived from the type of x.
        return iter_type([f"{prefix}{i}{suffix}" for i in x])  # type: ignore
    else:
        raise ValueError(f"x must be iterable. Passed type = {type(x)}.")


def broadcast_type(
    x: Iterable[Any], type_func: Union[Callable[[Any], Any], Type]
) -> Iterable[Any]:
    """
    Applies the passed type conversion function to each element in the passed
    list. Note that if you pass isnumeric plus broadcast_type has special
    functionality and will use the results of isnumericplus to determine what
    type to convert numeric strings to.

    Args:
        x (Iterable[Any]): Any iterable to broadcast types over.
        type_func (Union[Callable[[Any], Any], Type]): A python type (int, float,
            etc) or any Callable that takes a single argument and returns a
            single object.

    Returns:
        Iterable[Any]: x with type_func applied to each element.

    Raises:
        ValueError: If a non-iterable is passed for x.
    """
    if isinstance(x, Iterable):
        result = []
        iter_type = type(x)
        for val in x:
            if type_func.__name__ == "isnumericplus":
                _, t = isnumericplus(val, return_type=True)
            else:
                t = type_func
            result.append(t(val))
        # This must be flagged as type: ignore because pyright doesn't like the
        # variability of the flexible constructor derived from the type of x.
        return iter_type(result)  # type: ignore
    else:
        raise ValueError(f"x must be iterable. Passed type = {type(x)}.")


def collect_by_keys(x: Dict[Any, Any], *keys: Any) -> Dict[Any, Any]:
    """
    A simple function to collect an arbitrary and not-necessarily ordered subset
    of a dictionary.

    Args:
        x (Dict[Any, Any]): Any dict-like.

    Returns:
        Dict[Any, Any]: A dictionary or OrderedDict containing only the passed
            keys. Returns an object of the same type passed.
    """
    result = type(x)()
    for k, v in x.items():
        if k in keys:
            result[k] = v
    return result


def gen_alpha_keys(num: int) -> List[str]:
    """
    Generates a set of characters from the Latin alphabet a la excel
    headers.

    Args:
        num (int): The desired length of the set.

    Returns:
        List[str]: A list containing as many letters and letter combos as
            desired. Can be used to generate sets up to 676 in length.
    """
    a = string.ascii_uppercase
    result = list()
    x = num // 26
    for i in range(x + 1):
        root = a[i - 1] if i > 0 else ""
        keys = [root + a[j] for j in range(26)]
        for k in keys:
            result.append(k) if len(result) < num else None
    return result


def tuplify(value: Any) -> Tuple[Any, ...]:
    """
    Simple function that puts the passed object value into a tuple, if it is not
    already.

    Args:
        value (Any): Any object.

    Returns:
        Tuple[Any, ...]: The passed value as a tuple.
    """
    if not isinstance(value, tuple):
        # Covers dicts and OrderedDicts:
        if isinstance(value, dict):
            value = [(k, v) for k, v in value.items()]
        # Ensures it doesn't accidentally unpack strings or iterables:
        elif not isinstance(value, Iterable) or isinstance(value, str):
            value = [value]
        return tuple(value)
    else:
        return value


def tuplify_iterable(
    value: Union[Dict[Any, Any], MutableSequence[Any]]
) -> Union[Dict[Any, Tuple[Any]], MutableSequence[Tuple[Any]]]:
    """
    Convenience function for broadcasting tuplify over the elements of a
    dictionary or mutable sequence.

    Args:
        value (Union[Dict[Any, Any], MutableSequence[Any]]): Dict-like or list-
            like.

    Returns:
        Union[Dict[Any, Tuple[Any]], MutableSequence[Tuple[Any]]]: The passed
            dictionary (with values wrapped in tuples) or mutable sequence (with
            elements wraped in tuples).
    """
    # Covers dicts and OrderedDicts.
    if isinstance(value, dict):
        iterable = value.items()
    else:
        iterable = enumerate(value)
    for k, v in iterable:
        value[k] = tuplify(v)
    return value


def withinplus(within: Iterable, *values: Any) -> bool:
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
