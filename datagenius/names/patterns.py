import os
from pathlib import Path
from typing import Dict, List, Optional
import string

import yaml


class Patterns:
    def __init__(self) -> None:
        raw = self.load_patterns()
        self._compound_fnames = raw.get("compound_fnames", [])
        self._lname_particles = raw.get("lname_particles", [])
        self._ampersands = raw.get("ampersands", [])
        self._camelcase_particles = raw.get("camelcase_particles", [])
        self._prefixes = raw.get("prefixes", [])
        self._suffixes = raw.get("suffixes", [])
        self._invalid_words = raw.get("invalid_words", [])
        self._invalid_chars = self._get_invalid_chars()

    @property
    def compound_fnames(self) -> List[str]:
        return self._compound_fnames

    @property
    def lname_particles(self) -> List[str]:
        return self._lname_particles

    @property
    def ampersands(self) -> List[str]:
        return self._ampersands

    @property
    def camelcase_particles(self) -> List[str]:
        return self._camelcase_particles

    @property
    def prefixes(self) -> List[str]:
        return self._prefixes

    @property
    def suffixes(self) -> List[str]:
        return self._suffixes

    @property
    def invalid_chars(self) -> List[str]:
        return self._invalid_chars

    @property
    def invalid_words(self) -> Optional[List[str]]:
        return self._invalid_words

    @classmethod
    def load_patterns(cls) -> Dict[str, List[str]]:
        p = Path("datagenius/names/patterns")
        pattern_files = os.listdir(p)
        results = dict()
        for f in pattern_files:
            with open(p.joinpath(f), "r") as r:
                y = yaml.load(r, Loader=yaml.Loader)
                results = {**results, **y}
        return results

    def add_custom_pattern_file(self, p: Path) -> None:
        custom = self._load_custom_pattern(p)
        self._update_patterns(**custom)

    @staticmethod
    def _get_invalid_chars() -> List[str]:
        invalid_chars = [p for p in string.punctuation]
        for x in ("&", "'", "-", "."):
            invalid_chars.remove(x)
        return invalid_chars

    @staticmethod
    def _load_custom_pattern(p: Path) -> Dict[str, List[str]]:
        if p.suffix not in (".yml", ".yaml"):
            raise ValueError(f"custom_pattern_file {p} must be a .yml or .yaml file.")
        else:
            with open(p, "r") as r:
                y = yaml.load(r, Loader=yaml.Loader)
            for v in y.values():
                if not isinstance(v, list):
                    raise ValueError(
                        f"custom_pattern_file must contain only list objects. {v} is "
                        "invalid."
                    )
            return y

    def _update_patterns(self, **pattern_lists) -> None:
        for k, v in pattern_lists.items():
            prop = getattr(self, k, [])
            prop += v
            setattr(self, f"_{k}", prop)
