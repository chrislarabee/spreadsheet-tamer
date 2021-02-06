from typing import Any, Tuple
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
