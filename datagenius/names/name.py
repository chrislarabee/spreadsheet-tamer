from __future__ import annotations

from typing import Optional, List, Callable, Union, Dict, Any
import re
import pandas as pd

from datagenius.config import config

NameOperation = Callable[[str, Optional[int]], str]


class Name:
    def __init__(
        self,
        name: Union[str, List[str], List[Optional[str]]],
        operation_list: Optional[List[NameOperation]] = None,
    ) -> None:
        """
        Acts as a central repository for all the information about a given
        name. Whether it is valid, and what features it contains. This lets
        the core functions of Name parsing and validation be inherited by
        the real workhorses of parsing, Namestring and Nametoken.
        -
        Args:
            name (Union[str, List[Optional[str]]]): The name to assign to this
                Name object.
            operation_list (Optional[List[NameOperation]], optional): Additional
                NameOperations to use when parsing the passed name. Defaults to
                None.
        """
        self.original_name = None
        self.name_list = []
        self._intake(name)
        self.operations = [
            Name.cleanse_invalid_chars,
            Name.cleanse_invalid_word,
            Name.manage_cases,
        ]
        self.operations += operation_list if operation_list else []
        # Name 1 info:
        self.prefix = None
        self.fname = None
        self.mname = None
        self.lname = None
        self.suffix = None
        self.alt_name = None
        # Name 2 info:
        self.name2 = None
        self.prefix2 = None
        self.fname2 = None
        self.mname2 = None
        self.lname2 = None
        self.suffix2 = None
        self.alt_name2 = None
        # Meta attributes:
        self.valid = True
        # Process name:
        self._do_operations()

    def _allocate(self) -> None:
        """
        Assigns name components to attributes based on position in the
        name_list.
        """
        self.fname = self.name_list[0]
        self.mname = self.name_list[1]
        self.lname = self.name_list[2]

    def _do_operations(self):
        """
        Executes the assigned NameOperations if the Name is valid.
        """
        for operation in self.operations:
            self._validate()
            if self.valid:
                self._loop_name_list(operation)
            else:
                break

    def _intake(self, name: Union[str, List[str], List[Optional[str]]]):
        """
        Takes a passed name, which must either be a string or a list of strings,
        and assigns it to self.name_list after a small amount of pre-processing.
        -
        Args:
            name (Union[str, List[str]]): Name to preprocess.
        """
        self.original_name = name
        if isinstance(name, str):
            name = self.standardize_hyphen(name)
            self.name_list = name.lower().split(" ")
        else:
            self.name_list = []
            for n in name:
                if pd.isna(n):
                    n = None
                if n is not None:
                    n = self.standardize_hyphen(n)
                    n = n.lower()
                self.name_list.append(n)

    def _loop_name_list(self, operation: NameOperation):
        """
        Loops over each element in self.name_list and applies the passed function
        to it. If the passed function returns '', then that result is treated as
        an indication that the string should be removed from self.name_list.
        -
        Args:
            operation (NameOperation): A NameOperation, defined as a function
                that takes an integer and a string as positional arguments and
                returns a string.
        """
        to_remove = []
        for i, string in enumerate(self.name_list):
            if string is not None:
                result = operation(string, i)
            else:
                result = string
            if result == "":
                to_remove.append(string)
            else:
                self.name_list[i] = result
        for invalid in to_remove:
            self.name_list.remove(invalid)

    def _validate(self, final: Optional[bool] = False):
        """
        Checks the name list for validity. If any of the conditions specified in
        _validate pass, then the entire Name is not valid.
        -
        Args:
            final (Optional[bool], optional): True to check if the required
                name_attributes are None. Defaults to False.
        """
        m = re.search("[0-9]", str(self.name_list))
        if m is not None:
            self.valid = False
        if len(self.name_list) < 2:
            self.valid = False
        # Called with final=True after allocation:
        if final:
            req_names = ["fname", "lname"]
            one_char_len_count = 0
            for n in req_names:
                attr = getattr(self, n)
                if attr is None:
                    self.valid = False
                elif len(attr) == 1:
                    one_char_len_count += 1
            if one_char_len_count > 1:
                self.valid = False

    @staticmethod
    def cleanse_invalid_chars(s: str, index: Optional[int] = None) -> str:
        """
        Takes a string and removes all invalid characters (as specified by
        datagenius patterns configuration) from it.
        -
        Args:
            s (str): The string to cleanse.
            index (Optional[int], optional): Index in name_list where the string
                was found. Required for compatibility as a NameOperation.
                Defaults to None.
        -
        Returns:
            str: The cleansed string
        """
        for char in config.patterns.invalid_chars:
            s = s.replace(char, "")
        return s

    @staticmethod
    def cleanse_invalid_word(s: str, index: Optional[int] = None) -> str:
        """
        Takes a string and, if it is one of the invalid words specified in
            datagenius patterns configuration, returns ''.
        -
        Args:
            s (str): The string to cleanse.
            index (Optional[int], optional): Index in name_list where the string
                was found. Required for compatibility as a NameOperation.
                Defaults to None.
        -
        Returns:
            str: The string, or '' if it is an invalid word.
        """
        string_list = s.split(" ")
        invalid = []
        for s in string_list:
            if s.lower() in config.patterns.invalid_words:
                invalid.append(s)
        for s in invalid:
            string_list.remove(s)
        if len(string_list) > 0:
            return " ".join(string_list)
        else:
            return ""

    @staticmethod
    def format_camelcase(name: str, hump_start: int) -> str:
        """
        Camelcases a name that uses nonstandard capitalization (e.g. McElroy).
        -
        Args:
            name (str): The name to camelcase
            hump_start (int): The first character of the "hump" in the string.
                (i.e. in McElroy the "hump" starts at index 2).
        -
        Returns:
            str: Camel-cased version of name.
        """
        prefix = name[:hump_start]
        if hump_start == len(name):
            remainder = ""
        else:
            remainder = name[hump_start].upper() + name[hump_start + 1 :]
        return prefix + remainder

    @classmethod
    def manage_cases(cls, s: str, index: Optional[int] = None) -> str:
        """
        Makes sure each the passed string is properly capitalized.
        -
        Args:
            s (str): The string to cleanse.
            index (Optional[int], optional): Index in name_list where the string
                was found. Required for compatibility as a NameOperation.
                Defaults to None.
        -
        Returns:
            str: Camel-cased version of name.
        """
        # Check for and process camel-cased type names:
        for particle in config.patterns.camelcase_particles:
            m = re.search(particle + "[a-z]", s)
            if m is not None:
                s = cls.format_camelcase(s, m.end() - 1)
        # Check for and process multi-part names.
        separators = [" ", "-"]
        for sep in separators:
            s = cls.search_and_split(s, sep)
        s = cls.standardize_caps(s)
        return s

    def to_dict(self, record_dict: Dict[str, Any]) -> None:
        """
        Takes a dictionary and assigns the Name object's attributes to
        matching keys from the dictionary
        -
        Args:
            record_dict (Dict[str, Any]): A dictionary containing keys that match
            some or all of Name's attributes.
        """
        self._allocate()
        self._validate(True)
        for key in record_dict.keys():
            attr = getattr(self, key, None)
            if attr is not None:
                record_dict[key] = attr

    def to_list(self, force_name2: bool = False) -> List[Optional[str]]:
        """
        Generates a list of the name's components.
        -
        Args:
            force_name2 (bool): True to include name2 values in the list even if
                they are all null. Will be a list of length 10 instead of length
                5. Default is False.
        -
        Returns:
            List[Optional[str]]: A list consisting of the name's prefix, first
                name, middle name, last name, and suffix.
        """
        self._allocate()
        self._validate(True)
        name1 = [self.prefix, self.fname, self.mname, self.lname, self.suffix]
        if (self.fname2 and self.lname2) or force_name2:
            name2 = [self.prefix2, self.fname2, self.mname2, self.lname2, self.suffix2]
        else:
            name2 = []
        return name1 + name2

    @staticmethod
    def search_and_split(s: str, search_char: str) -> str:
        """
        Takes a string and a single character search value and uses re.search to
        check for that search_char. If it finds it, it splits string on that
        value and then standardizes the capitalization of each element in the
        string, before joining it all back together on the search_char.
        -
        Args:
            s (str): The string to search.
            search_char (str): A single character to split s on.
        -
        Returns:
            str: The string with capitalization standardized if search_char was
                found.
        """
        m = re.search(search_char, s)
        if m is not None:
            string_list = s.split(search_char)

            for i, s in enumerate(string_list):
                string_list[i] = Name.standardize_caps(s)
            s = search_char.join(string_list)
        return s

    @staticmethod
    def standardize_caps(s: str) -> str:
        """
        Takes a string and capitalizes the first character.
        -
        Args:
            s (str): Any string.
        -
        Returns:
            str: The string with first character capitalized.
        """
        if len(s) < 2:
            s = s.upper()
        else:
            s = s[0].upper() + s[1:]
        return s

    @staticmethod
    def standardize_hyphen(s: str) -> str:
        """
        Ensures any hyphens present fit the required standard of:
            char-char
            NOT char -char or char- char.
        -
        Args:
            s (str): The string to check for hyphens and standardize.
        -
        Returns:
            str: The passed string, but with any hyphens standardized
        """
        s = s.replace("- ", "-")
        s = s.replace(" -", "-")
        return s
