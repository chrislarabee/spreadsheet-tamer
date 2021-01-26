import os
from pathlib import Path
from typing import Dict, List, Optional

import yaml

from datagenius import config


def load_patterns() -> Dict[str, List[str]]:
    p = Path('datagenius/names/patterns')
    pattern_files = os.listdir(p)
    results = dict()
    for f in pattern_files:
        with open(p.joinpath(f), 'r') as r:
            y = yaml.load(r, Loader=yaml.Loader)
            results = {**results, **y}
    if config.custom_pattern_file:
        custom = load_custom_pattern(Path(config.custom_pattern_file))
        for k, v in custom.items():
            if results.get(k):
                results[k] += v
            else:
                results[k] = v
    return results


def load_custom_pattern(p: Path) -> Optional[Dict[str, List[str]]]:
    if p.suffix not in ('.yml', '.yaml'):
        raise ValueError(
            f'custom_pattern_file {p} must be a .yml or .yaml file.'
        )
    else:
        with open(p, 'r') as r:
            y = yaml.load(r, Loader=yaml.Loader)
        for v in y.values():
            if not isinstance(v, list):
                raise ValueError(
                    f'custom_pattern_file must contain only list objects. {v} is '
                    'invalid.'
                )
        return y