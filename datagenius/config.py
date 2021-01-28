from typing import Optional
from pathlib import Path

from datagenius.names.patterns import Patterns


patterns = Patterns()

name_colums = [
    "prefix1",
    "fname1",
    "mname1",
    "lname1",
    "suffix1",
    "prefix2",
    "fname2",
    "mname2",
    "lname2",
    "suffix2",
]


def configure(*, custom_pattern_file_path: Optional[str] = None) -> None:
    if custom_pattern_file_path:
        patterns.add_custom_pattern_file(Path(custom_pattern_file_path))
