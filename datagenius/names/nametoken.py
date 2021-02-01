from typing import List, Optional

from datagenius.config import config
from datagenius.names.name import Name


class Nametoken(Name):
    def __init__(self, name_list: List[Optional[str]]) -> None:
        """
        Expands Name's functionality so that it can more robustly handle datasets
        where the name information is stored as separated into different types of
        names (fname, mname, lname, etc).
        -
        Args:
            name_list (List[Optional[str]]): The name as a list of names.
        """
        operation_list = [
            self.assign_trailing_middle_initial,
            self.assign_ampersand_split,
        ]
        super(Nametoken, self).__init__(name_list, operation_list)

    def _allocate(self) -> None:
        """
        Assigns name components to attributes based on position in name_list and
        length of name_list
        """
        self.fname = self.name_list[0]
        if len(self.name_list) == 3:
            if self.mname is None:
                self.mname = self.name_list[1]
            self.lname = self.name_list[2]
        elif len(self.name_list) == 2:
            self.lname = self.name_list[1]
        if self.fname2 is not None and self.lname2 is None:
            self.lname2 = self.lname

    def assign_ampersand_split(self, s: str, index: int) -> str:
        """
        Takes a string from name_list and checks it for ampersands. If it finds
        any it splits the string following the ampersand off into the appropriate n
        ame2 location (based on where it is in name_list. Removes everything
        after the ampersand and returns the cleaned string.
        -
        Args:
            s (str): A string from name_list to check for ampersands.
            index (int): Index in name_list where the string was found.
        -
        Returns:
            str: The cleansed string.
        """
        amp_indices = []
        ampersands = []
        string_list = s.split(" ")
        for i, s in enumerate(string_list):
            if s.lower() in config.patterns.ampersands:
                amp_indices.append(i)
                ampersands.append(s)
        if len(amp_indices) > 0:
            name_list2 = string_list[amp_indices[0] + 1 :]
            string_list = string_list[: amp_indices[0]]
            name2 = " ".join(name_list2)
            if index == 0:
                self.fname2 = name2
            elif index == 1:
                self.mname2 = name2
            else:
                self.lname2 = name2
        return " ".join(string_list)

    def assign_trailing_middle_initial(self, s: str, index: int) -> str:
        """
        It's common for tokenized names to have a middle initial at the end of
        the first name token, for whatever reason, so this method moves it to the
        mname and removes it from the passed string.
        -
        Args:
            s (str): The string to check for trailing middle initials.
            index (int): Index in name_list where the string was found.
        -
        Returns:
            str: The string, with its trailing initial removed if one is found.
        """
        string_list = s.split(" ")
        if index == 0:
            if len(string_list) > 1:
                last_string = string_list[-1]
                if len(last_string) == 1 or (
                    len(last_string) == 2 and last_string[1] == "."
                ):
                    self.mname = last_string
                    string_list.remove(last_string)
        return " ".join(string_list)
