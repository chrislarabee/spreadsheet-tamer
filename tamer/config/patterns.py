import os
from pathlib import Path
from typing import List, Dict
import string

import yaml

mod_path = Path(__file__).parent


class Patterns:
    def __init__(self) -> None:
        """
        Stores the various string patterns used by Name and its subclasses to
        parse names.
        """
        raw = self._load_patterns()
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
        """
        Returns:
            List[str]: Common multi-string first names like "Fun Fun" or "Mary Lou".
        """
        return self._compound_fnames

    @property
    def lname_particles(self) -> List[str]:
        """
        Returns:
            List[str]: Common particles like "Van" in "Van Houten".
        """
        return self._lname_particles

    @property
    def ampersands(self) -> List[str]:
        """
        Returns:
            List[str]: The various forms ampersands can take ("and", "&", etc.).
        """
        return self._ampersands

    @property
    def camelcase_particles(self) -> List[str]:
        """
        Returns:
            List[str]: Common last name particles like the Mc in McElroy that
                indicate a need for camelcased last names.
        """
        return self._camelcase_particles

    @property
    def prefixes(self) -> List[str]:
        """
        Returns:
            List[str]: Words that should be treated as prefixes. If you customize,
                add periods where appropriate.
        """
        return self._prefixes

    @property
    def suffixes(self) -> List[str]:
        """
        Returns:
            List[str]: Words that should be treated as suffixes. If you customize,
                add periods where approrpriate.
        """
        return self._suffixes

    @property
    def invalid_chars(self) -> List[str]:
        """
        Returns:
            List[str]: Punctuation characters that should not be allowed in names.
        """
        return self._invalid_chars

    @property
    def invalid_words(self) -> List[str]:
        """
        Returns:
            List[str]: Words that are not allowed in names, like "Subscriber" or
                very obvious organization names.
        """
        return self._invalid_words

    @classmethod
    def _load_patterns(cls) -> Dict[str, List[str]]:
        """
        Loads the pattern _config_files into a dictionary of lists.
        -
        Returns:
            Dict[str, List[str]]: A dictionary with keys matching Patterns'
                properties and lists of strings to assign to those properties.
        """
        p = mod_path.joinpath("_config_files/patterns").resolve()
        pattern_files = os.listdir(p)
        results = dict()
        for f in pattern_files:
            with open(p.joinpath(f), "r") as r:
                y = yaml.load(r, Loader=yaml.Loader)
                results = {**results, **y}
        return results

    def add_custom_pattern_file(self, p: Path) -> None:
        """
        Loads a yaml file stored at an arbitrary Path and assigns keys to
        matching Patterns properties.
        -
        Args:
            p (Path): Path to a valid yaml file.
        """
        custom = self._load_custom_pattern(p)
        self._update_patterns(**custom)

    @staticmethod
    def _get_invalid_chars() -> List[str]:
        """
        Generates a list of characters invalid in names.
        -
        Returns:
            List[str]: A list of invalid characters.
        """
        invalid_chars = [p for p in string.punctuation]
        for x in "&'-.":
            invalid_chars.remove(x)
        return invalid_chars

    @staticmethod
    def _load_custom_pattern(p: Path) -> Dict[str, List[str]]:
        """
        Acts like _load_patterns but with more checking to ensure the end user is
        supplying an appropriately formatted yaml file.

        Args:
            p (Path): Path to the custom yaml file.

        Raises:
            ValueError: For files that are not yml or yaml files.
            ValueError: For yaml files that don't contain dictionary and list
                style syntax.

        Returns:
            Dict[str, List[str]]: A dictionary with keys matching Patterns'
                properties and lists of strings to assign to those properties.
        """
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
        """
        Updates Patterns' properties using the passed pattern kwargs.
        """
        for k, v in pattern_lists.items():
            prop = getattr(self, k, None)
            if prop:
                prop += v
                setattr(self, f"_{k}", list(set(prop)))
