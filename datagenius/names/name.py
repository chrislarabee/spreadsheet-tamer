import re
import pandas as pd

# from config import INVALID_CHARS, INVALID_WORDS, CAMELCASE_PARTICLES


class Name:
    """
    Acts as a central repository for all the information about a given
    name. Whether it is valid, and what features it contains. This lets
    the core functions of Name parsing and validation be inherited by
    the real workhorses of parsing, Namestring and Nametoken.
    :param name: A string or list of strings/Nones.
    :param operation_list: A list of functions, which must accept only
        a single string argument and which must return a string.
    """
    def __init__(self, name, operation_list=[]):
        self.original_name = None
        self.name_list = None
        self._intake(name)
        self.operations = [
            Name.cleanse_invalid_chars,
            Name.cleanse_invalid_word,
            Name.manage_cases
        ]
        self.operations += operation_list
        # Name 1 info:
        self.prefix = None
        self.fname = None
        self.mname = None
        self.lname = None
        self.suffix = None
        # Name 2 info:
        self.prefix2 = None
        self.fname2 = None
        self.mname2 = None
        self.lname2 = None
        self.suffix2 = None
        # Meta attributes:
        self.valid = True
        # Process name:
        self._do_operations()
        
    def _allocate(self):
        """
        Assigns name components to attributes based on position in the
        name_list. Override this method during inheritance.
        :return: None
        """
        self.fname = self.name_list[0]
        self.mname = self.name_list[1]
        self.lname = self.name_list[2]

    def _do_operations(self):
        for operation in self.operations:
            self._validate()
            if self.valid:
                self._loop_name_list(operation)
            else:
                break

    def _intake(self, name):
        """
        Takes a passed name, which must either be a string or a list of
        strings, and assigns it to self.name_list after a small amount
        of pre-processing.
        :param name: A string or a list of strings.
        :return: None
        """
        self.original_name = name
        if isinstance(name, str):
            name = self.standardize_hyphen(name)
            self.name_list = name.lower().split(' ')
        else:
            self.name_list = []
            for n in name:
                if pd.isna(n):
                    n = None
                if n is not None:
                    n = self.standardize_hyphen(n)
                    n = n.lower()
                self.name_list.append(n)

    def _loop_name_list(self, function):
        """
        Loops over each element in self.name_list and applies the
        passed function to it. If the passed function returns '', then
        that result is treated as an indication that the string should
        be removed from self.name_list.
        :param function: A function, which can take two arguments
            AS LONG AS the second argument is a single string
            argument. Function MUST return a string argument.
        :return: None
        """
        to_remove = []

        for i, string in enumerate(self.name_list):
            if string is not None:
                result = function(i, string)
            else:
                result = string

            if result == '':
                to_remove.append(string)
            else:
                self.name_list[i] = result

        for invalid in to_remove:
            self.name_list.remove(invalid)

    def _validate(self, final=False):
        """
        Checks the name list for validity. If any of the conditions
        specified in _validate pass, then the entire Name is not valid.
        :param final: A boolean that tells the function whether to check
        if the required name_attributes are none.
        :return: None
        """
        m = re.search('[0-9]', str(self.name_list))
        if m is not None:
            self.valid = False
        if len(self.name_list) < 2:
            self.valid = False
        # Called with final=True after allocation:
        if final:
            req_names = ['fname', 'lname']
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
    def cleanse_invalid_chars(*args):
        """
        Takes a string and removes all invalid characters (as specified
        by INVALID_CHARS in config) from it.
        :param args: Any number of args as long as the last one is a
            string.
        :return: The cleansed string.
        """
        string = args[-1]
        for char in INVALID_CHARS:
            string = string.replace(char, '')
        return string

    @staticmethod
    def cleanse_invalid_word(*args):
        """
        Takes a string and, if it is one of the invalid words specified
        in INVALID_WORDS in config, returns ''.
        :param args: Any number of args as long as the last one is a
            string.
        :return: The string, or '' if it is an invalid word.
        """
        string = args[-1]
        string_list = string.split(' ')
        invalid = []
        for s in string_list:
            if s.lower() in INVALID_WORDS:
                invalid.append(s)

        for s in invalid:
            string_list.remove(s)

        if len(string_list) > 0:
            return ' '.join(string_list)
        else:
            return ''

    @staticmethod
    def format_camelcase(name, hump_start):
        """
        Takes a name as a string and a Match object from re.search and uses
        that to camel-case the name.
        :param name: A string.
        :param hump_start: The first character of the "hump" in the string.
            (i.e. in McElroy the "hump" starts at index 2).
        :return: Camel-cased version of name.
        """
        prefix = name[:hump_start]
        if hump_start == len(name):
            remainder = ''
        else:
            remainder = name[hump_start].upper() + name[hump_start + 1:]
        return prefix + remainder

    @classmethod
    def manage_cases(cls, *args):
        """
        Makes sure each the passed string is properly capitalized.
        :param *args: Any number of arguments as long as the last one
            is a string.
        :return: The case-corrected string.
        """
        string = args[-1]
        # Check for and process camel-cased type names:
        for particle in CAMELCASE_PARTICLES:
            m = re.search(particle + "[a-z]", string)
            if m is not None:
                string = cls.format_camelcase(string, m.end() - 1)

        # Check for and process multi-part names.
        separators = [' ', '-']
        for sep in separators:
            string = cls.search_and_split(string, sep)

        string = cls.standardize_caps(string)

        return string

    def populate(self, record_dict):
        """
        Takes a dictionary and assigns the Name object's attributes to
        matching keys from the dictionary
        :param record_dict: A dictionary containing keys that match
            some or all of Name's attributes.
        :return: None
        """
        self._allocate()
        self._validate(True)
        for key in record_dict.keys():
            attr = getattr(self, key, None)
            if attr is not None:
                record_dict[key] = attr

    @staticmethod
    def search_and_split(string, search_char):
        """
        Takes a string and a single character search value and uses
        re.search to check for that search_char. If it finds it, it
        splits string on that value and then standardizes the
        capitalization of each element in the string, before joining
        it all back together on the search_char.
        :param string: A string.
        :param search_char: A single character string to search with.
        :return: The string with capitalization standardized if
            search_char was found.
        """
        m = re.search(search_char, string)
        if m is not None:
            string_list = string.split(search_char)

            for i, s in enumerate(string_list):
                string_list[i] = Name.standardize_caps(s)
            string = search_char.join(string_list)
        return string

    @staticmethod
    def standardize_caps(string):
        """
        Takes a string and capitalizes the first character.
        :param string: A string
        :return: The string with first character capitalized.
        """
        if len(string) < 2:
            string = string.upper()
        else:
            string = string[0].upper() + string[1:]

        return string

    @staticmethod
    def standardize_hyphen(string):
        """
        Ensures any hyphens present fit the required standard of:
            char-char
            NOT char -char or char- char.
        """
        string = string.replace('- ', '-')
        string = string.replace(' -', '-')
        return string
