from typing import Tuple, Any, Optional, Type, TypeVar
import re

import numpy as np

from .numerics.zero_numeric import ZeroNumeric
from .decorators import nullable


Numeric = TypeVar("Numeric", int, float, np.int64, np.float32, np.float64)


@nullable
def convertplus(obj: Any, target_type: Type) -> Any:
    """
    Smarter type conversion that avoids errors when converting to numeric from
    non-standard strings and which can be used in pd.Series.apply calls.
    -
    Args:
        obj (Any): Any object.
        target_type (Type): The target type to convert to.
    -
    Returns:
        Any: The passed object converted to the target type.
    """
    if isinstance(obj, str) and target_type == float and isnumericplus(obj):
        pts = re.search(r"\.+", obj)
        point_ct = len(pts.group()) if pts else 0
        if point_ct > 1:
            obj = re.sub(r"\.+", ".", obj)
    return target_type(obj)


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


@nullable
def type_plus(obj: Any) -> Type:
    """
    Wrapper for type that distinguishes nan values as nan and not float.
    -
    Args:
        obj (Any): Any object.
    -
    Returns:
        Any: The type of the object, or nan if it is a numpy nan.
    """
    return type(obj)
