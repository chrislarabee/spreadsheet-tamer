import os
from pathlib import Path
from typing import Dict, List, Optional, Any, Union, Tuple
import string

import yaml


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
        p = Path("./datagenius/_config_files/patterns")
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
        for x in ("&", "'", "-", "."):
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
            prop = getattr(self, k, [])
            prop += v
            setattr(self, f"_{k}", prop)


class GConfig:
    def __init__(self) -> None:
        """
        One stop shop for all configuration used by datagenius. Customize to
        change core functionality.
        """
        self._patterns = Patterns()
        raw = self._load_config()
        self._name_columns = tuple(raw.get("name_columns", []))

    @property
    def patterns(self) -> Patterns:
        """
        Returns:
            Patterns: The Patterns object used by the names package to parse
                names.
        """
        return self._patterns

    @property
    def name_column_labels(self) -> Tuple[str, str, str, str, str]:
        """
        Returns:
            Tuple[str, str, str, str, str]: The labels for the five name strings
                generated by parse_name_string_column and parse_tokenized_names
                transmutations (default is prefix, fname, mname, lname, suffix)
        """
        return self._name_columns

    @name_column_labels.setter
    def name_column_labels(self, value: Tuple[str, str, str, str, str]) -> None:
        if len(value) != 5:
            raise ValueError("name_column_labels must be a tuple of length 5.")
        if isinstance(value, tuple):
            for v in value:
                if not isinstance(v, str):
                    raise ValueError(
                        f"All elements of name_column_labels must be strings. {v} is "
                        f"type {type(v)}"
                    )
            self._name_columns = value
        else:
            raise ValueError(
                f"name_column_labels must be a tuple. Passed value type is {type(value)}"
            )

    def add_custom_pattern_file(self, p: Union[str, Path]) -> None:
        """
        Add a custom yaml file with name patterns unique to your dataset. You can
        supply any of the properties of GConfig.patterns as keys in the yaml,
        and then any strings as list values for those keys.
        -
        Args:
            p (Union[str, Path]): The path to your custom yaml file.
        """
        p = Path(p)
        self._patterns.add_custom_pattern_file(p)

    @classmethod
    def _load_config(cls) -> Dict[str, Any]:
        """
        Loads datagenius' general config file.

        Returns:
            Dict[str, Any]: A dictionary containing keys matching GConfig
                properties and values to assign to those properties.
        """
        p = Path("./datagenius/_config_files/config.yml")
        with open(p, "r") as r:
            results = yaml.load(r, Loader=yaml.Loader)
        return results
