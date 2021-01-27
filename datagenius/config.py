from typing import Optional
from pathlib import Path

import datagenius.names.util as name_utils


patterns = name_utils.Patterns()

def configure(
    *,
    custom_pattern_file_path: Optional[str] = None
) -> None:
    if custom_pattern_file_path:
        patterns.add_custom_pattern_file(Path(custom_pattern_file_path))

