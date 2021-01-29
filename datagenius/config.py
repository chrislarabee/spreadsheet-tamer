from os import name
from typing import Optional, Tuple
from pathlib import Path

from datagenius.names.patterns import Patterns


patterns = Patterns()

name_columns = (
    "prefix",
    "fname",
    "mname",
    "lname",
    "suffix",
)


def configure(
        *, 
        custom_pattern_file_path: Optional[str] = None,
        name_column_labels: Optional[Tuple[str, str, str, str, str]] = None
) -> None:
    """
    Use to configure datagenius behavior. 
    -
    Args:
        custom_pattern_file_path (Optional[str], optional): Use this to supply a
            custom pattern yaml file to Name objects. The contents of that file 
            will be added to the existing Name patterns. Defaults to None.
        name_column_labels (Optional[Tuple[str, str, str, str, str]], optional): 
            Use this to override the default five name column labels used by the 
            Names transmutations. Defaults to None.
    -
    Raises:
        ValueError: Will raise a ValueError if you supply an invalid argument 
            value. Individual errors will explain the offending argument value.
    """
    if custom_pattern_file_path:
        patterns.add_custom_pattern_file(Path(custom_pattern_file_path))
    if name_column_labels:
        if len(name_column_labels) != 5:
            raise ValueError(f"name_column_labels must be a tuple of 5 strings.")
        else:
            global name_columns
            name_columns = name_column_labels
