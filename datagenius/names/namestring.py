from typing import Optional
import re

from datagenius.config import patterns
from datagenius.names import Name


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
        super(Namestring, self).__init__(name_string, operation_list)
        self.name_list1 = None
        self.name_list2 = None
        if self.valid:
            self.manage_multi_fname()
            self.manage_multi_lname()
            self.assign_ampersand_split()

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
        datagenius Patterns configuration. Places the first match it finds in
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
        affixes = {"prefix": patterns.prefixes, "suffix": patterns.suffixes}
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
            if string.lower() in patterns.ampersands:
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
                if combo.lower() in patterns.compound_fnames:
                    self.name_list[i] = combo
                    absorbed.append(name2)
        for string in absorbed:
            self.name_list.remove(string)

    def manage_multi_lname(self) -> None:
        """
        Takes a Name object and checks it for common last name "particles" such
        as in "Van Houten". If found, it overwrites the name_list at the
        appropriate locations. Cannot be called as part of _do_operations because
        more than one token in the name_list is needed for it to run.
        """
        absorbed = []
        chain = []
        for i, string in enumerate(self.name_list):
            if string.lower() in patterns.lname_particles and string not in absorbed:
                if i < len(self.name_list) - 1:
                    for name2 in self.name_list[i + 1 :]:
                        if name2 not in absorbed:
                            chain.append(name2)
                            absorbed.append(name2)
                            if name2.lower() not in patterns.lname_particles:
                                break
                    self.name_list[i] += " " + " ".join(chain)

        for string in absorbed:
            self.name_list.remove(string)
