from typing import List, Sequence, Any
import string
import re


class Header:
    def __init__(self, *labels) -> None:
        """
        Use this to represent your list of header row labels instead of another
        sequence to guarantee your header labels will be standardized and unique.

        Args:
            labels: Any type of Sequence, either expanded with * or passed as a
                single object. You can also simply pass the labels as args.
        """
        if len(labels) == 1:
            labels = labels[0]
        self._original = [*labels]
        self._header = self._standardize(labels)
        self._i = 0

    @property
    def original_form(self) -> List[Any]:
        """
        Returns:
            List[Any]: The original contents of the labels passed on init as a
                List.
        """
        return self._original

    def append(self, value: str) -> None:
        """
        Adds the passed value to the end of Header.

        Args:
            value (str): The value to append.
        """
        self._original.append(value)
        self._header.append(value)
        self._header = self._standardize(self._header)

    def pop(self, key: int) -> str:
        """
        Deletes the Header value at the passed key.

        Args:
            key (int): The index of the value to delete.

        Raises:
            IndexError: If the passed key is out of range.

        Returns:
            str: The deleted value.
        """
        if key >= len(self):
            raise IndexError(f"Header.pop({key}): {key} out of range")
        else:
            return self.__delitem__(key)

    def remove(self, value: str) -> None:
        """
        Deletes the passed value from the Header.

        Args:
            value (str): The value to delete.

        Raises:
            ValueError: If the passed value is not present in the Header or the
                original_form of the Header.
        """
        if value in self._header:
            idx = self._header.index(value)
        elif value in self._original:
            idx = self._original.index(value)
        else:
            raise ValueError(f"Header.remove({value}): {value} not in Header.")
        self.__delitem__(idx)

    @staticmethod
    def _enforce_uniques(x: List[Any]) -> List[str]:
        """
        Ensures the passed list has no duplicate values by appending a numeral to
        the end of any duplicates (e.g. x, x_1, x_2, etc).

        Args:
            x (List[Any]): A list of any contents.

        Returns:
            List[str]: The passed list, but with any duplicates converted to
                strings and with their contents appended with _n.
        """
        values = dict()
        for i, val in enumerate(x):
            if val in values.keys():
                x[i] = str(val) + "_" + str(values[val])
                values[val] += 1
            else:
                values[val] = 1
        return x

    @classmethod
    def _standardize(cls, header: Sequence) -> List[str]:
        """
        Standardizes the passed Sequence, converting it to a list of unique
        strings with no non-alphanumeric characters except _.

        Args:
            header (Sequence): Any iterable sequence.

        Returns:
            List[str]: The passed Sequence as a list of standardized strings.
        """
        result = []
        p = string.punctuation.replace("_", "")
        for h in header:
            h = str(h)
            h = re.sub(re.compile(r"[" + p + "]"), "", h)
            result.append(re.sub(" +", "_", h.strip()).lower())
        if len(set(result)) < len(result):
            result = cls._enforce_uniques(result)
        return result

    def __repr__(self) -> str:
        return str(self._header)

    def __getitem__(self, key: int) -> str:
        return self._header[key]

    def __setitem__(self, key: int, item: str) -> None:
        self._header[key] = item
        self._original[key] = item
        self._header = self._standardize(self._header)

    def __delitem__(self, key: int) -> str:
        x = self._header.pop(key)
        self._original.pop(key)
        return x

    def __len__(self) -> int:
        return len(self._header)

    def __iter__(self):
        self._i = 0
        return self

    def __next__(self):
        if self._i < len(self):
            result = self[self._i]
            self._i += 1
            return result
        else:
            raise StopIteration

    def __eq__(self, o: object) -> bool:
        if self._header == o:
            return True
        else:
            return False

    def __ne__(self, o: object) -> bool:
        if self._header != o:
            return True
        else:
            return False
