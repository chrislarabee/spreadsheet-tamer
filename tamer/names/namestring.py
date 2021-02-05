from typing import Optional, Tuple, List
import re

from tamer.config import config
from tamer.names.name import Name


class Namestring(Name):
    def __init__(self, name_string: str) -> None:
        """
        Expands Name's functionality so that it can more robustly handle datasets
        where the name information is stored as a single string.
        -
        Args:
            name_string (str): The name as a single string.
        """
        operation_list = [self.assign_middle_initials, self.assign_affix]
        self.chain = []
        self.mid_init_clusters = 1
        name_string, alt_name1, alt_name2 = self.extract_alt_name(name_string)
        super(Namestring, self).__init__(name_string, operation_list)
        if alt_name1 and not self.alt_name:
            self.alt_name = self.manage_cases(alt_name1)
        if alt_name2 and not self.alt_name2:
            self.alt_name2 = self.manage_cases(alt_name2)
        self.name_list1 = None
        self.name_list2 = None
        if self.valid:
            self.assign_ampersand_split()
            self.manage_multi_fname()
            if self.name_list1:
                self.name_list1 = self.manage_multi_lname(self.name_list1)
            else:
                self.name_list = self.manage_multi_lname(self.name_list)
            if self.name_list2:
                self.name_list2 = self.manage_multi_lname(self.name_list2)

    def _allocate(self) -> None:
        """
        Assigns name components to attributes based on position in name_list1 and
        name_list2.
        """
        if self.name_list1 is None:
            self.name_list1 = self.name_list
        self.fname = self.name_list1[0]
        lname_idx = None
        if len(self.name_list1) > 2:
            if self.mname is None:
                self.mname = self.name_list1[1]
            lname_idx = 2
        elif len(self.name_list1) == 2:
            lname_idx = 1
        if lname_idx is not None:
            self.lname = " ".join(self.name_list[lname_idx:])
        if self.name_list2 is not None:
            self.fname2 = self.name_list2[0]
            if len(self.name_list2) > 2:
                if self.mname2 is None:
                    self.mname2 = self.name_list2[1]
                lname_idx = 2
            else:
                lname_idx = 1
            self.lname2 = " ".join(self.name_list2[lname_idx:])
            if self.lname is None:
                self.lname = self.lname2

    def assign_affix(self, s: str, index: Optional[int] = None) -> str:
        """
        Takes a passed string and checks it against prefixes and suffixes of
        datagenius patterns configuration. Places the first match it finds in
        prefix/suffix and the second it finds in prefix2/suffix2.
        :return:
        -
        Args:
            s (str): The string to check for affixes.
            index (Optional[int], optional): Index in name_list where the string
                was found. Required for compatibility as a NameOperation.
                Defaults to None.
        -
        Returns:
            str: The string if no matches were found, '' otherwise.
        """
        affixes = dict(prefix=config.patterns.prefixes, suffix=config.patterns.suffixes)
        output = s
        for key, match_against in affixes.items():
            matches = list(filter(re.compile(s.lower()).match, match_against))
            if len(matches) > 0:
                if getattr(self, key) is None:
                    setattr(self, key, s)
                elif getattr(self, key + "2") is None:
                    setattr(self, key + "2", s)
                output = ""
        return output

    def assign_ampersand_split(self) -> None:
        """
        Checks for ampersands in the name_list and extracts the name2 information
        if one is found. There's not a great way to handle more than 1 ampersand
        though, so it will probably return some crazy results if you hit a record
        with more than one. Cannot be part of _do_operations because it needs to
        use indices to split the name_list.
        """
        amp_indices = []
        ampersands = []
        for i, string in enumerate(self.name_list):
            if string.lower() in config.patterns.ampersands:
                amp_indices.append(i)
                ampersands.append(string)
        for i in amp_indices:
            if i != 0:
                self.name_list2 = self.name_list[i + 1 :]
                self.name_list1 = self.name_list[:i]
        for amp in ampersands:
            if amp in self.name_list:
                self.name_list.remove(amp)
            if self.name_list1 is not None:
                if amp in self.name_list1:
                    self.name_list1.remove(amp)
            if self.name_list2 is not None:
                if amp in self.name_list2:
                    self.name_list2.remove(amp)

    def assign_middle_initials(self, s: str, index: int) -> str:
        """
        Takes a passed string and checks if it looks like a single-char initial.
        Builds a cluster of matches at the object level until it stops finding
        matches, and then assigns the cluster to mname or mname2.
        -
        Args:
            s (str): The string to check for single character initials.
            index (int): Index in name_list where the string was found.
        -
        Returns:
            str: The string if no matches were found, '' otherwise.
        """
        output = s
        m = re.search("[a-z]", s, re.IGNORECASE)
        if m is not None and index > 0:
            if len(s) == 1 or (len(s) == 2 and s[1] == "."):
                if len(s) == 1:
                    s += "."
                self.chain.append(s)
                output = ""
            elif len(self.chain) > 0:
                if self.mid_init_clusters == 1:
                    suffix = ""
                else:
                    suffix = str(self.mid_init_clusters)
                setattr(self, "mname" + suffix, "".join(self.chain))
                self.chain = []
                self.mid_init_clusters += 1
        return output

    def manage_multi_fname(self) -> None:
        """
        Takes a Name object and checks it for common multi-part first names. If
        found, it overwrites the name_list at the appropriate locations. Cannot
        be called as part of _do_operations because more than one token in the
        name_list is needed for it to run.
        """
        absorbed = []
        for i, string in enumerate(self.name_list):
            if i < len(self.name_list) - 1:
                name2 = self.name_list[i + 1]
                combo = string + " " + name2
                for match_against in config.patterns.compound_fnames:
                    match = re.match(re.compile(match_against), combo.lower())
                    if match:
                        self.name_list[i] = combo
                        absorbed.append(name2)
        for string in absorbed:
            self.name_list.remove(string)

    @staticmethod
    def manage_multi_lname(name_list: List[str]) -> List[str]:
        """
        Takes a Name object and checks it for common last name "particles" such
        as in "Van Houten". If found, it overwrites the name_list at the
        appropriate locations. Cannot be called as part of _do_operations because
        more than one token in the name_list is needed for it to run.
        """
        chain = []
        lname_start = None
        for i, string in enumerate(name_list):
            # Check the string against each lname_particle regex and add it to
            # the chain on match. If a chain has been started and a non-match is
            # found, assume that this string is the final part of the last name
            # chain.
            for match_against in config.patterns.lname_particles:
                match = re.match(re.compile(match_against), string.lower())
                if match or lname_start:
                    lname_start = i if not lname_start else lname_start
                    chain.append(string)
                    break
        for c in chain:
            name_list.remove(c)
        if lname_start:
            name_list.insert(lname_start, " ".join(chain))
        return name_list

    @staticmethod
    def extract_alt_name(s: str) -> Tuple[str, Optional[str], Optional[str]]:
        """
        Pulls names in parentheses () out of the passed string.

        Args:
            s (str): A string that may or may not have parentheses in it.

        Returns:
            Tuple[str, Optional[str], Optional[str]]: The string, with parentheses
                removed, and up to two additional strings containing the first
                two names in parentheses found by the method.
        """
        paren_s = r" *\(.+\) *"
        match = re.search(paren_s, s)
        alt_name1 = None
        alt_name2 = None
        while match:
            match_start = match.start()
            substr = s[match_start:]
            close_paren = re.match(r".*?\) *", substr).end()
            match_end = close_paren + match_start
            alt_name = s[match_start:match_end]
            alt_name = re.sub(r"[\(\)]", "", alt_name).strip()
            if alt_name1 is None:
                alt_name1 = alt_name
            elif alt_name2 is None:
                alt_name2 = alt_name
            s = f"{s[:match.start()].strip()} {s[match_end:].strip()}"
            match = re.search(paren_s, s)
        return s, alt_name1, alt_name2
