import collections as col
from abc import ABC

from datagenius.io import reader


class Element(col.abc.Sequence, ABC):
    """
    A superclass for Element objects, which allows implementation
    of some methods needed by all the objects in this module.
    """
    def __init__(self, data: (list, dict, col.OrderedDict)):
        """

        Args:
            data: A list, dict, or OrderedDict (depends on the
                exact specifications of the Element child class).
        """
        self.data = data

    def element_comparison(self, other,
                           eq_result: bool = True) -> bool:
        """
        Whenever Element objects are compared to other Elements,
        compare their object reference. Whenever they're compared
        to any other objects, compare the Element's data attribute
        to that object.

        Args:
            other: Any object.
            eq_result: A boolean, switch to False when using
                element_comparison to implement !=.

        Returns: A boolean.

        """
        if isinstance(other, self.__class__):
            if self.__repr__() == other.__repr__():
                result = eq_result
            else:
                result = not eq_result
        else:
            if self.data == other:
                result = eq_result
            else:
                result = not eq_result
        return result

    def __eq__(self, other) -> bool:
        """
        Overrides built-in object equality so that Elements
        used in == statements compare the value in self.data
        rather than the Element object itself.

        Args:
            other: Any object.

        Returns: A boolean indicating whether the value of self.data
            is equivalent to other.

        """
        return self.element_comparison(other)

    def __ne__(self, other) -> bool:
        """
        Overrides built-in object inequality so that Elements
        used in != statements compare the value in self.data
        rather than the Element object itself.

        Args:
            other: Any object.

        Returns: A  boolean indicating whether the value of self.data
            is not equivalent to other.

        """
        return self.element_comparison(other, False)

    def __getitem__(self, item):
        return self.data[item]

    def __len__(self):
        return len(self.data)


class Dataset(Element):
    """
    A wrapper object for lists of lists. Datasets are the primary
    data-containing object for datagenius.
    """
    def __init__(self, data: list):
        """
        Datasets must be instantiated with a list of lists.

        Args:
            data: A list of lists.
        """
        struct_error_msg = ('Dataset data must be instantiated as a ' \
                            'list of lists.')
        if isinstance(data, list):
            if isinstance(data[0], list):
                self.col_ct = len(data[0])
                for d in data:
                    if len(d) != self.col_ct:
                        raise ValueError(f'All rows must have the '
                                         f'same length. Invalid row= '
                                         f'{d}')
                super(Dataset, self).__init__(data)
                self.header = None
                self.format = 'lists'
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

    def to_format(self, to: str):
        """
        Triggers the passed format change.

        Args:
            to: A string found in format_funcs dict, below.

        Returns:

        """
        format_funcs = {
            'dicts': self.to_dicts,
            'lists': self.to_lists
        }
        prev_format = self.format
        format_funcs[to]()
        if prev_format != self.format:
            return True
        else:
            return False

    @staticmethod
    def from_file(file_path: str):
        """
        Uses read_file to read in the passed file path.

        Args:
            file_path: The file path to the desired data file.

        Returns: For excel workbooks with multiple sheets, it will
            return a dictionary of sheet names as keys and raw
            sheet contents as values. For excel workbooks with
            one sheet and other file formats with a single set of
            data, it will return a Dataset object.

        """
        raw = reader.read_file(file_path)
        if len(raw.keys()) == 1:
            return Dataset(list(raw.values())[0])
        else:
            return raw

    def remove(self, key: (int, list)) -> None:
        """
        Removes a row from the dataset based on its index or
        an exact match of the rows contents.

        Args:
            key: An integer corresponding to an index in self.data
                or a list corresponding to a row in self.data.

        Returns: None

        """
        if isinstance(key, int):
            self.data.pop(key)
        elif isinstance(key, list):
            self.data.remove(key)
        else:
            raise ValueError('Dataset.remove can only take int '
                             'or list arguments.')

    def to_dicts(self):
        """
        Uses self.header and self.data to convert self.data into
        a list of OrderedDicts instead of a list of lists.

        Returns: self, with self.data modified to be a list of
            OrderedDicts.

        """
        if self.header is None:
            raise AttributeError('This Dataset has no header. '
                                 'Cannot convert to dicts format '
                                 'without a header.')
        elif self.format == 'lists':
            results = []
            for row in self:
                d = col.OrderedDict()
                for i, h in enumerate(self.header):
                    d[h] = row[i]
                results.append(d)
            self.data = results
            self.format = 'dicts'
        return self

    def to_lists(self):
        """
        Converts self.data back into a list of lists and uses
        the keys of the first row as the new header (in cases
        changes were made).

        Returns: self, with self.data modified to be a list of
            lists.

        """
        if self.format == 'dicts':
            results = []
            self.header = list(self[0].keys())
            for row in self:
                results.append([*list(row.values())])
            self.data = results
            self.format = 'lists'
        return self


class MappingRule(Element):
    """
        A fairly simple object for encoding what string value an object
        corresponds to and what default value should be used if a given row
        in the dataset governed by the Rule has no value.
        """

    def __init__(self, to: str = None, default=None):
        """

        Args:
            to: A string, representing the column/field/key this
                MappingRule corresponds to in a Dataset.
            default: The value to use if a row in a Dataset returns
                no value from application of this MappingRule.
        """
        self.to = to
        self.default = default
        super(MappingRule, self).__init__(
            {'to': self.to, 'default': self.default})

    def __call__(self, value=None):
        """
        Applies the MappingRule to a passed value.

        Args:
            value: Any value the MappingRule needs to be applied
                to.

        Returns: The column the value should be mapped to, and the
            value (replaced with self.default if value is None).

        """
        if value is None:
            v = self.default
        else:
            v = value
        return self.to, v


class Mapping(Element):
    """
    Provides a quick way for creating many MappingRules from
    a target format template and from explicitly detailed rules.
    """
    def __init__(self, template: (list, tuple),
                 rules: (dict, col.OrderedDict)):
        """
        Args:
            template: A list or tuple of strings, the header
                of the target format you want a Dataset to be
                converted into.
            rules: A dict or OrderedDict with keys from template
                and values being strings or MappingRules indicating
                which columns in a Dataset should map to which
                columns in template.
        """
        super(Mapping, self).__init__(dict())
        self.template = template
        """
        Don't be tempted to make rules into **rules! Too many
        datasets have column names with spaces in them, and your
        target format might require that!
        """
        for k, v in rules.items():
            if k not in self.template:
                raise ValueError(f'All passed rule keys must '
                                 f'be in the passed template. Bad '
                                 f'key: {k}')
            elif not isinstance(v, MappingRule):
                if isinstance(v, str):
                    r = MappingRule(v)
                else:
                    raise ValueError(f'Passed values must be '
                                     f'strings or MappingRule '
                                     f'objects. Bad kv pair: {k, v}')
            else:
                r = v
            self.data[k] = r

        # Ensure all template keys are used even if they are not
        # mapped:
        for t in self.template:
            if t not in self.data.keys():
                self.data[t] = MappingRule()
