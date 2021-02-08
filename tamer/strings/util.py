from typing import Any, Tuple, Sequence
import re


def clean_whitespace(x: Any) -> Tuple[bool, Any]:
    """
    When passed a string, removes leading and trailing whitespace from it and
    also replaces any chains of more than one space with a single space.
    -
    Args:
        x (Any): Any object.
    -
    Returns:
        Tuple[bool, Any]: A tuple containing a boolean that indicates whether x was
            cleaned, and x, cleaned of whitespace if applicable.
    """
    cleaned = False
    clean_x = x
    if isinstance(clean_x, str):
        clean_x = re.sub(r" +", " ", x.strip())
        cleaned = True if clean_x != x else False
    return cleaned, clean_x


def count_true_str(x: Sequence) -> int:
    """
    Takes a list-like and returns the number of values in it that are strings and 
    that are not ''.
    -
    Args:
        x (Sequence): List-like object to count strings in.
    -
    Returns:
        int: The count of non-blank strings.
    """
    return sum([1 if isinstance(y, str) and y != "" else 0 for y in x])
