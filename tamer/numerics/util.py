from typing import Union, Tuple, Any, Optional, Type, List
import re

from .zero_numeric import ZeroNumeric


def isnumericplus(x: Any, return_type: bool = False) -> Tuple[bool, Optional[Type]]:
    """
    A better version of the str.isnumeric test that correctly identifies floats 
    stored as strings as numeric.
    -
    Args:
        x (Any): Any object.
        return_type (bool, optional): Causes isnumericplus to return the type of x. Defaults to False.
    -
    Returns:
        Tuple[bool, Optional[Type]]: A tuple of a boolean and a type, if
            return_type was set to True.
    """
    numeric = False
    result = []
    v = type(x)
    if isinstance(x, (int, float, ZeroNumeric)):
        numeric = True
    elif isinstance(x, str):
        v = int if re.search(r"^-*\d+$", x) else v
        v = ZeroNumeric if x[0] == "0" and x not in ("0", "0.00") else v
        v = float if re.search(r"^-*\d+\.+\d*$", x) else v
        numeric = True if v in (int, float, ZeroNumeric) else False
    result.append(numeric)
    if return_type:
        result.append(v)
    return tuple(result) if len(result) > 1 else result[0]
