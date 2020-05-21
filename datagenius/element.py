import collections as col
import os
from abc import ABC

from datagenius.io import text, odbc
import datagenius.util as u


class MetaData(col.abc.MutableMapping, ABC):
    """
    Stores meta data on the columns of a Dataset object and provides
    convenience methods for updating and interacting with the meta
    data.
    """
    def __init__(self, data: dict = None):
        """

        Args:
            data: A dictionary.
        """
        self.data: dict = data if data is not None else dict()

    def clear(self, column: str = None) -> None:
        """
        Clears all the meta data for the given column or the entire
        MetaData object if no column is passed.

        Args:
            column: A string, a key found in self.data.

        Returns: None

        """
        if column is None:
            self.data = dict()
        else:
            self.pop(column)

    def update(self, column: str, **kwargs) -> None:
        """
        Convenience method for updating the MetaData's information
        for a given column. Can add as many key-value pairs as desired
        to the column's entry in self.data.

        Args:
            column: A string.
            **kwargs: Key value pairs to add to the sub-dictionary
                found in self.data[column]

        Returns: None

        """
        if not self.get(column):
            self.data[column] = dict()
        self[column] = {**self[column], **kwargs}

    def __delitem__(self, key):
        self.data.pop(key)

    def __getitem__(self, item):
        return self.data[item]

    def __iter__(self):
        return self.data.__iter__()

    def __len__(self):
        return len(self.data)

    def __setitem__(self, key, value):
        self.data[key] = value


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
        self.data: (list, dict, col.OrderedDict) = data

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
    def __init__(self, data: list, header: list = None):
        """
        Datasets must be instantiated with a list of lists.

        Args:
            data: A list of lists.
            header: A list, the header of the Dataset. Mostly used as a
                convenience attribute for testing.
        """
        struct_error_msg = (
            'Dataset data must be instantiated as a list of lists or '
            'OrderedDicts.')
        self.header: (list, None) = None
        self.data_format: (str, None) = None
        self.data_orientation: str = 'row'
        # Stores rows when parsers reject them and need to store them:
        self.rejects: list = []
        # Stores results from Explore objects.
        self.meta_data: MetaData = MetaData()
        if isinstance(data, list):
            row1 = data[0]
            self.row_ct: int = len(data)
            self.col_ct: int = len(row1)
            if isinstance(row1, list):
                self.header = self._gen_temp_header(header)
                self.data_format = 'lists'
            elif isinstance(row1, col.OrderedDict):
                self.header = list(row1.keys())
                self.data_format = 'dicts'
            else:
                raise ValueError(struct_error_msg)
            for row in data:
                if len(row) != self.col_ct:
                    raise ValueError(
                        f'All rows must have the same length. '
                        f'Invalid row={row}')
            super(Dataset, self).__init__(data)
        else:
            raise ValueError(struct_error_msg)

    def _gen_temp_header(self, manual_header: list) -> list:
        """

        Returns: A list of numbers as strings as long as the dset is
        wide.

        """
        if manual_header:
            return manual_header
        elif self.header is None:
            return [str(i) for i in range(self.col_ct)]

    def copy(self):
        """
        Creates a copy of the Dataset with the same data and header.

        Returns: A copy of the Dataset object.

        """
        d = Dataset(self.data.copy())
        if self.header:
            d.header = self.header.copy()
        return d

    def transpose(self) -> None:
        """
        Transposes the Dataset's data, making the columns rows and the
        rows columns.

        Returns: None

        """
        self.to_format('lists')
        self.data = list(map(list, zip(*self.data)))
        if self.data_orientation == 'row':
            self.data_orientation = 'column'
        else:
            self.data_orientation = 'row'

    def to_format(self, to: str) -> bool:
        """
        Triggers the passed data_format change.

        Args:
            to: A string found in format_funcs dict, below.

        Returns: A boolean indicating whether to_format
            needed to execute a formatting function.

        """
        format_funcs = {
            'dicts': self.to_dicts,
            'lists': self.to_lists
        }
        prev_format = self.data_format
        format_funcs[to]()
        return True if prev_format != self.data_format else False

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
        raw = text.read_file(file_path)
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
        elif isinstance(key, (list, col.OrderedDict)):
            self.data.remove(key)
        else:
            raise ValueError(
                'Dataset.remove can only take int or list/OrderedDict '
                'arguments.')

    def to_dicts(self):
        """
        Uses self.header and self.data to convert self.data into
        a list of OrderedDicts instead of a list of lists.

        Returns: self, with self.data modified to be a list of
            OrderedDicts.

        """
        if self.header is None:
            raise AttributeError(
                'This Dataset has no header. Cannot convert to dicts '
                'data_format without a header.')
        elif self.data_format == 'lists':
            results = []
            for row in self:
                d = col.OrderedDict()
                for i, h in enumerate(self.header):
                    d[h] = row[i]
                results.append(d)
            self.data = results
            self.data_format = 'dicts'
        return self

    def to_lists(self):
        """
        Converts self.data back into a list of lists and uses
        the keys of the first row as the new header (in cases
        changes were made).

        Returns: self, with self.data modified to be a list of
            lists.

        """
        if self.data_format == 'dicts':
            results = []
            self.header = list(self[0].keys())
            for row in self:
                results.append([*list(row.values())])
            self.data = results
            self.data_format = 'lists'
        return self

    def to_file(self, dir_path: str, output_name: str, to: str = 'sqlite',
                **options):
        """
        Converts the dataset into dicts data_format and then writes its
        data to a local sqlite db or to a csv file.

        Args:
            dir_path: A string, the directory to locate the sqlite db
                or csv file.
            output_name: A string, the name of the csv file or table in
                the sqlite db to use.
            to: A string, either 'sqlite' or 'csv'.
            **options: Key-value options to alter to_file's behavior.
                Currently in use options:
                    db_conn: An io.odbc.ODBConnector object if you have
                        one already, otherwise to_file will create one.
                    db_name: A string to specifically name the db to
                        output to. Default is 'datasets'

        Returns: None

        """
        if self.data_orientation != 'row':
            self.transpose()
        self.to_dicts()
        f, ext = os.path.splitext(output_name)
        if to == 'csv':
            ext = 'csv' if ext == '' else ext
            p = os.path.join(dir_path, f + '.' + ext)
            text.write_csv(p, self.data, self.header)
        elif to == 'sqlite':
            p = os.path.join(dir_path, options.get('db_name', 'datasets') + '.db')
            type_map = {
                'uncertain': str,
                'numeric': float,
                'string': str,
                'integer': int
            }
            if len(self.meta_data) != len(self.header):
                raise ValueError(
                    'Discrepancy between meta_data and header. Pass '
                    'this Dataset through genius.Explore to generate '
                    'meta_data or supply prob_type meta_data for each '
                    'column manually.')
            else:
                schema = {
                    k: type_map[v['probable_type']] for k, v in self.meta_data.items()}
                o = options.get('db_conn', odbc.ODBConnector())
                o.setup(p)
                o.drop_tbl(f)
                o.insert(f, self, schema)
                # Add meta_data tables for this dataset:
                dset_md_tbl = f + '_dset_meta_data'
                col_md_tbl = f + '_col_meta_data'
                o.drop_tbl(dset_md_tbl)
                o.drop_tbl(col_md_tbl)
                (dset_md, dset_md_schema,
                 col_md, col_md_schema) = self.meta_data_report()
                o.insert(dset_md_tbl, dset_md, dset_md_schema)
                o.insert(col_md_tbl, col_md, col_md_schema)
        else:
            raise ValueError(
                f'Unrecognized "to": {to}'
            )

    def meta_data_report(self):
        """
        Produces a tuple of values related to the Dataset's meta_data
        attribute and meta data on the Dataset itself, as well as
        schemas for each of those objects so that they can be easily
        written to sqlite if desired.

        Returns: A tuple of:
            dataset_md: A list of dictionaries containing meta data
                features of the Dataset.
            dataset_md_schema: A dictionary schema for dataset_md.
            column_md: A list of dictionaries containing meta data for
                each column described in self.meta_data.
            column_md_schema: A dictionary schema for column_md.

        """
        column_md = []
        column_md_schema = dict()
        for k, v in self.meta_data.items():
            column_md_schema['column'] = str
            result = col.OrderedDict(
                column=k
            )
            for vk, vv in v.items():
                column_md_schema[vk] = str
                if isinstance(vv, (list, tuple)):
                    result[vk] = str(vv)[:30]
                else:
                    result[vk] = vv
            if len(column_md) > 0:
                if set(result.keys()) != set(column_md[-1].keys()):
                    raise ValueError(
                        f'Inconsistent keys for column meta_data. '
                        f'Invalid meta_data info = {k, v}')
            column_md.append(result)

        reject_val_ct = sum([u.non_null_count(x) for x in self.rejects])
        dataset_md_features = {
            'Number of columns': f'{self.col_ct}',
            'Number of rows': f'{self.row_ct}',
            'Number of rejected rows': f'{len(self.rejects)}',
            'Number of values in rejected rows': f'{reject_val_ct}'
        }
        dataset_md = [
            {'feature': k, 'value': v} for k, v in dataset_md_features.items()
        ]
        dataset_md_schema = {'feature': str, 'value': str}
        return dataset_md, dataset_md_schema, column_md, column_md_schema


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
                of the target data_format you want a Dataset to be
                converted into.
            rules: A dict or OrderedDict with keys from template
                and values being strings or MappingRules indicating
                which columns in a Dataset should map to which
                columns in template.
        """
        super(Mapping, self).__init__(dict())
        self.template = template
        """
        Don't be tempted to make rules into **rules! Too many datasets 
        have column names with spaces in them, and your target 
        data_format might require that!
        """
        for k, v in rules.items():
            if k not in self.template:
                raise ValueError(
                    f'All passed rule keys must be in the passed '
                    f'template. Bad key: {k}')
            elif not isinstance(v, MappingRule):
                if isinstance(v, str):
                    r = MappingRule(v)
                else:
                    raise ValueError(
                        f'Passed values must be strings or MappingRule '
                        f'objects. Bad kv pair: {k, v}')
            else:
                r = v
            self.data[k] = r

        # Ensure all template keys are used even if they are not
        # mapped:
        for t in self.template:
            if t not in self.data.keys():
                self.data[t] = MappingRule()
