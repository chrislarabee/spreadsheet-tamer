import collections
from abc import ABC

import datagenius.util as u


class Dataset(collections.abc.Sequence, ABC):
    """
    A wrapper object for lists of lists. Datasets are the primary
    data-containing object for datagenius.
    """
    def __init__(self, data: list, **kwargs):
        """
        Datasets must be instantiated with a list of lists.

        Args:
            data: A list of lists.
            kwargs: Various ways to customize Dataset's behaviors.
                Currently in use kwargs:
                threshold: An integer, the number of
                    non-null/blank values that a row must have to
                    be included in the dataset. By default this will
                    be the number of columns in the dataset - 1
                    in order to automatically weed out obvious
                    subtotal rows.
        """
        struct_error_msg = 'Dataset data must be a list of lists.'
        if isinstance(data, list):
            if isinstance(data[0], list):
                self.col_ct = len(data[0])
                for d in data:
                    if len(d) != self.col_ct:
                        raise ValueError(f'All rows must have the '
                                         f'same length. Invalid row= '
                                         f'{d}')
                self.data = data
                self.header = None
                # Attributes to allow iteration.
                self.cur_idx = -1
                self.max_idx = len(data)
            else:
                raise ValueError(struct_error_msg)
        else:
            raise ValueError(struct_error_msg)

    def copy(self):
        """
        Creates a copy of the Dataset with the same data
        and header.

        Returns: A copy of the Dataset object.

        """
        d = Dataset(self.data.copy())
        if self.header:
            d.header = self.header.copy()
        return d

    def __eq__(self, other) -> bool:
        """
        Overrides built-in object equality so that Datasets
        used in == statements compare the list in self.data
        rather than the Dataset object itself.

        Args:
            other: Any object.

        Returns: A boolean indicating whether the value of self.data
            is equivalent to other.

        """
        result = False
        if isinstance(other, Dataset):
            if self.__repr__() == other.__repr__():
                result = True
        else:
            if self.data == other:
                result = True
        return result

    def __getitem__(self, item):
        return self.data[item]

    def __len__(self):
        return len(self.data)

    def __ne__(self, other) -> bool:
        """
        Overrides built-in object inequality so that Dataset's
        used in != statements compare the list in self.data
        rather than the Dataset object itself.

        Args:
            other: Any object.

        Returns: A  boolean indicating whether the value of self.data
            is not equivalent to other.

        """
        result = False
        if isinstance(other, Dataset):
            if self.__repr__() != other.__repr__():
                result = True
        else:
            if self.data != other:
                result = True
        return result
