from typing import Optional
from pathlib import Path

from datagenius.names.patterns import Patterns


patterns = Patterns()

def configure(
    *,
    custom_pattern_file_path: Optional[str] = None
) -> None:
    if custom_pattern_file_path:
        patterns.add_custom_pattern_file(Path(custom_pattern_file_path))

