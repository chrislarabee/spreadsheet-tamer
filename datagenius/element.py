import collections as col
import inspect
import os
import re
from abc import ABC
from typing import Callable

import pandas as pd
import recordlinkage as link

from datagenius.io import text, odbc
import datagenius.util as u


class Element(ABC):
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
        self._data: (list, dict, col.OrderedDict, pd.DataFrame) = data

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
            if self._data == other:
                result = eq_result
            else:
                result = not eq_result
        return result

    def __eq__(self, other) -> bool:
        """
        Overrides built-in object equality so that Elements
        used in == statements compare the value in self._data
        rather than the Element object itself.

        Args:
            other: Any object.

        Returns: A boolean indicating whether the value of self._data
            is equivalent to other.

        """
        return self.element_comparison(other)

    def __ne__(self, other) -> bool:
        """
        Overrides built-in object inequality so that Elements
        used in != statements compare the value in self._data
        rather than the Element object itself.

        Args:
            other: Any object.

        Returns: A  boolean indicating whether the value of self._data
            is not equivalent to other.

        """
        return self.element_comparison(other, False)

    def __getitem__(self, item):
        return self._data[item]

    def __len__(self):
        return len(self._data)

    def __str__(self):
        return f'{self.__repr__()}({self._data})'


class MetaData(Element, col.abc.MutableMapping):
    """
    Stores meta data on the columns of a Dataset object and provides
    convenience methods for updating and interacting with the meta
    data.
    """
    def __init__(self, data: dict = None, **init_attrs):
        """

        Args:
            data: A dictionary.
        """
        data = data if data is not None else dict()
        super(MetaData, self).__init__(data)
        self.header: list = list()
        self.header_idx: (int, None) = None
        self.init_row_ct: (int, None) = None
        self.init_col_ct: (int, None) = None
        self.white_space_cleaned: int = 0
        for k, v in init_attrs.items():
            if k in self.__dict__.keys():
                setattr(self, k, v)

    def copy(self):
        """
        Creates a copy of the MetaData.

        Returns: A copy of the MetaData object.

        """
        md = MetaData(
            self._data.copy(),
            header=self.header.copy(),
            init_row_ct=self.init_row_ct,
            init_col_ct=self.init_col_ct
        )
        return md

    def gen_temp_header(self, x: int,
                        manual_header: (list, None) = None) -> None:
        """
        Assigns a manual_header to self.header if passed, otherwise
        creates a temporary header with indices as strings.

        Returns: None

        """
        if manual_header:
            self.header = manual_header
        else:
            self.header = [str(i) for i in range(x)]

    def calculate(self, func, key: str, attr: (str, None) = None):
        """
        Applies the passed function to all the values in
        self._data's dictionaries stored at the passed key.

        Args:
            func: Any function that takes a single list argument.
            key: A string, the key of the values you want to pull from
                the dictionary of meta data for each column in
                self._data.
            attr: A string, the name of the attribute you wish to
                create on MetaData that will store the result of func's
                calculation. If None, the value will not be stored in an
                attribute.

        Returns: The return of func.

        """
        f = func([v[key] for v in self._data.values()])
        setattr(self, attr, f) if attr is not None else None
        return f

    def check_key(self, key: str) -> bool:
        """
        Checks all the column dictionaries in self._data and returns
        True if they all have the passed key and False if any of them
        don't.

        Args:
            key: A string, a key in self._data.

        Returns: A boolean.

        """
        x = {True if v.get(key) is not None else False for v in self.values()}
        return len(x) == 1 and list(x)[0]

    def clear_col_data(self, column: str = None) -> None:
        """
        Clears all the meta data for the given column or the entire
        MetaData object if no column is passed.

        Args:
            column: A string, a key found in self._data.

        Returns: None

        """
        if column is None:
            self._data.clear()
        else:
            self.pop(column)

    def update(self, column: str, **kwargs) -> None:
        """
        Convenience method for updating the MetaData's information
        for a given column. Can add as many key-value pairs as desired
        to the column's entry in self._data.

        Args:
            column: A string.
            **kwargs: Key value pairs to add to the sub-dictionary
                found in self._data[column]

        Returns: None

        """
        if not self.get(column):
            self[column] = dict()
        self[column] = {**self[column], **kwargs}

    def concat_header(self, new: list) -> list:
        """
        Used to append one or more values to header without duplicating
        existing values.

        Args:
            new: A list of values to append to header.

        Returns: A list, the altered header.

        """
        h_set = set(self.header)
        n_set = set(new)
        to_add = list(n_set.difference(h_set))
        self.header += to_add
        return self.header

    def update_attr(self, attr: str, value, _type=None) -> None:
        """
        Takes an attribute and a value, and optionally a type, and
        uses them to update an attribute on the MetaData object, or to
        create a new attribute.
        Args:
            attr: A string, the name of the target attribute.
            value: An object, the value to set attr to or to update
                attr with.
            _type: None, collections.OrderedDict, list, or dict. Pass
                one of the latter three to tell update_attr to create
                and empty attribute of that type and/or add value to
                that attribute rather than setting directly.

        Returns: None

        """
        update_funcs = {
            'dict_like': lambda x: setattr(
                self, attr, {**getattr(self, attr), **x}),
            'list': lambda x: getattr(self, attr).append(x),
            'other': lambda x: setattr(self, attr, x)
        }
        a = getattr(self, attr, None)
        t = 'other'
        if _type is None and a is not None:
            if isinstance(a, (col.OrderedDict, dict)):
                t = 'dict_like'
            elif isinstance(a, list):
                t = 'list'
        elif a is None and _type is not None:
            if _type in (col.OrderedDict, dict, list):
                setattr(self, attr, _type())
                t = 'list' if _type == list else 'dict_like'
        update_funcs[t](value)

    def __delitem__(self, key):
        self._data.pop(key)

    def __iter__(self):
        return self._data.__iter__()

    def __setitem__(self, key, value):
        self._data[key] = value


class Dataset(Element, col.abc.Sequence):
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
        self.data_format: (str, None) = None
        self.data_orientation: str = 'row'
        # Stores rows when parsers reject them and need to store them:
        self.rejects: list = list()
        # Stores results from Explore objects.
        self.meta_data: MetaData = MetaData()
        if isinstance(data, (tuple, list)):
            row1 = data[0]
            self.row_ct: int = len(data)
            self.col_ct: int = len(row1)
            if isinstance(row1, list):
                self.meta_data.gen_temp_header(self.col_ct, header)
                self.data_format = 'lists'
            elif isinstance(row1, col.OrderedDict):
                self.meta_data.header = list(row1.keys())
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

    def copy(self):
        """
        Creates a copy of the Dataset with the same data and meta_data.

        Returns: A copy of the Dataset object.

        """
        d = Dataset(self._data.copy())
        d.meta_data = self.meta_data.copy()
        return d

    def transpose(self, orientation: str) -> None:
        """
        Transposes the Dataset's data, making the columns rows and the
        rows columns.

        Args:
            orientation: A string, indicates what the desired
                data_orientation value is.

        Returns: None

        """
        orientation = 'row' if orientation == 'set' else orientation
        self.to_format('lists')
        if self.data_orientation != orientation:
            self._data = list(map(list, zip(*self._data)))
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
            'lists': self.to_lists,
            'df': self.to_df
        }
        if to == 'any':
            return False
        else:
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
            key: An integer corresponding to an index in self._data
                or a list corresponding to a row in self._data.

        Returns: None

        """
        if isinstance(key, int):
            self._data.pop(key)
        elif isinstance(key, (list, col.OrderedDict)):
            self._data.remove(key)
        else:
            raise ValueError(
                'Dataset.remove can only take int or list/OrderedDict '
                'arguments.')

    def to_df(self):
        """
        Converts self._data to OrderedDicts and then a pandas
        DataFrame.

        Returns: self, with self._data modified to be a pandas
            DataFrame.

        """
        if self.data_format != 'df':
            if self.data_format != 'dicts':
                self.to_dicts()
            self._data = pd.DataFrame(self._data)
            self.data_format = 'df'
        return self

    def to_dicts(self):
        """
        Uses self.header and self._data to convert self._data into
        a list of OrderedDicts.

        Returns: self, with self._data modified to be a list of
            OrderedDicts.

        """
        if self.meta_data.header is None:
            raise AttributeError(
                'This Dataset has no header. Cannot convert to dicts '
                'data_format without a header.')
        elif self.data_format == 'lists':
            results = []
            for row in self:
                d = col.OrderedDict()
                for i, h in enumerate(self.meta_data.header):
                    d[h] = row[i]
                results.append(d)
            self._data = results
            self.data_format = 'dicts'
        elif self.data_format == 'df':
            self.meta_data.header = list(self._data.columns)
            self._data = self._data.to_dict('records', into=col.OrderedDict)
            self._data = u.translate_nans(self._data)
            self.data_format = 'dicts'
        return self

    def to_lists(self):
        """
        Converts self._data into a list of lists and uses the keys of
        the first row as the new header (in case changes were made).

        Returns: self, with self._data modified to be a list of
            lists.

        """
        if self.data_format == 'dicts':
            results = []
            self.meta_data.header = list(self[0].keys())
            for row in self:
                results.append([*list(row.values())])
            self._data = results
            self.data_format = 'lists'
        elif self.data_format == 'df':
            self.meta_data.header = list(self._data.columns)
            self._data = self._data.values.tolist()
            self._data = u.translate_nans(self._data)
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
        self.transpose('row')
        self.to_dicts()
        f, ext = os.path.splitext(output_name)
        if to == 'csv':
            ext = 'csv' if ext == '' else ext
            p = os.path.join(dir_path, f + '.' + ext)
            text.write_csv(p, self._data, self.meta_data.header)
        elif to == 'sqlite':
            p = os.path.join(dir_path, options.get('db_name', 'datasets') + '.db')
            type_map = {
                'uncertain': str,
                'numeric': float,
                'string': str,
                'integer': int
            }
            schema = dict()
            for k in self.meta_data.header:
                v = self.meta_data.get(k)
                t = v['probable_type'] if v is not None else 'uncertain'
                schema[k] = type_map[t]
            o = options.get('db_conn', odbc.ODBConnector())
            o.setup(p)
            odbc.write_sqlite(o, f, self._data, schema)
            # Add meta_data tables for this dataset:
            dset_md_tbl = f + '_dset_meta_data'
            col_md_tbl = f + '_col_meta_data'
            (dset_md, dset_md_schema,
             col_md, col_md_schema) = self.meta_data_report()
            odbc.write_sqlite(o, dset_md_tbl, dset_md, dset_md_schema)
            odbc.write_sqlite(o, col_md_tbl, col_md, col_md_schema)
            # Add reject table for this dataset:
            reject_tbl = f + '_rejects'
            rejects, reject_schema = self.package_rejects()
            odbc.write_sqlite(o, reject_tbl, rejects, reject_schema)
        else:
            raise ValueError(
                f'Unrecognized "to": {to}'
            )

    def package_rejects(self):
        """
        Bundles rejects into dictionaries and generates a schema dict
        so that the rejects can be written to SQLite.

        Returns: A tuple containing a list of dictionaries (one for
            each row in self.rejects) and a dictionary containing a
            simple schema for the rejects table.

        """
        m = self.meta_data.header
        return (
            [col.OrderedDict(zip(m, r)) for r in self.rejects],
            dict(zip(m, [str for _ in range(len(m))]))
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

        reject_val_ct = sum([len(x) - u.count_nulls(x) for x in self.rejects])
        dataset_md_features = {
            'Number of columns': f'{self.col_ct}',
            'Number of rows': f'{self.row_ct}',
            'Number of rejected rows': f'{len(self.rejects)}',
            'Number of values in rejected rows': f'{reject_val_ct}',
            'Number of strings cleared of whitespace':
                f'{self.meta_data.white_space_cleaned}'
        }
        dataset_md = [
            {'feature': k, 'value': v} for k, v in dataset_md_features.items()
        ]
        dataset_md_schema = {'feature': str, 'value': str}
        return dataset_md, dataset_md_schema, column_md, column_md_schema

    def supplement(self, other, *on, exact: bool = True,
                   thresholds: tuple = None, select: tuple = None):
        """
        Adds columns from another Dataset to this Dataset.

        Args:
            other: The source Dataset to pull columns from.
            *on: Any number of keys, which must be in both Datasets,
                and which will be used to find matching rows in the two
                Datasets.
            exact: A boolean, if True, then only exact matches at on
                keys will be supplemented.
            thresholds: A tuple of floats from 0 to 1, used for inexact
                matching. Must be equal in length to on.
            select: A tuple of keys in other that are not in on. Keys in
                on will always be appended, but additional keys can be
                added with select.

        Returns: The Dataset with any values in matching rows added to
            existing rows.

        """
        self.to_df()
        other = other.copy().to_df()._data
        select = on if select is None else select
        # Can't join on nan values effective, so rows with nans in
        # on must be discarded from other:
        for o in on:
            other = other[~other[o].isna()]
        if exact:
            self._data = self._data.merge(
                other,
                'left',
                on=on,
                suffixes=('', '_s')
            )
        else:
            if thresholds is not None:
                if (isinstance(thresholds, (list, tuple))
                        and len(thresholds) != len(on)):
                    raise ValueError(
                        f'If provided, thresholds length must match on '
                        f'length: thresholds={thresholds}, on={on}')
                else:
                    raise ValueError(
                        f'If provided, thresholds must be a list or '
                        f'tuple: {thresholds}')
            else:
                thresholds = [.9 for _ in on]

            idxr = link.Index()
            idxr.full()
        drop_cols = (
            set(self._data.columns) - set(self.meta_data.header)
            - set(select))
        self._data.drop(columns=drop_cols, inplace=True)
        return self.to_dicts()


class Rule:
    """
    Highly flexible callable object that can store and execute
    functions designed to alter data at one or more keys in a passed
    OrderedDict.
    """
    def __init__(self,
                 rule_func: (str, Callable),
                 rule_iter: (dict, list, tuple) = None,
                 from_: (tuple, str) = None,
                 to: (tuple, str) = None):
        """

        Args:
            rule_func: The function to be applied to the values pulled
                using from_, or a string corresponding to a Rule method
                found in Rule.methods()
            from_: A string or tuple of keys to pull values from when
                passed an OrderedDict.
            rule_iter: An optional list, dictionary, or tuple that can
                help Rule execute rule_func. Note that this is NOT
                optional for certain built-in Rule methods.
            to: A string or tuple of keys to put values into once they
                have had rule_func applied to them. If to is not
                passed, altered values will be put directly back into
                their from_ keys. Also, if you want to broadcast a
                value from one from_ to multiple tos, you can do that,
                though you can't do the inverse.
        """
        # Collect the rule function:
        if isinstance(rule_func, str) and rule_func in self.methods().keys():
            rule_func = self.methods()[rule_func]
        elif isinstance(rule_func, (dict, list, tuple, str)):
            from_ = rule_iter
            rule_iter = rule_func
            rule_func = None
        self.rule: Callable = self._translate if rule_func is None else rule_func
        # Collect the rule iterable:
        if isinstance(rule_iter, (str, tuple)):
            from_ = rule_iter
            rule_iter = None
        self.translation: (dict, list) = self._prep_translation(rule_iter)
        # Collect from_:
        if isinstance(from_, str) or from_ is None:
            from_ = tuple([from_])
        self.from_: tuple = from_
        self.from_ct: int = len(self.from_)
        # Collect to:
        if isinstance(to, str):
            to = tuple([to])
        self.to: tuple = to
        self.to_ct: int = len(self.to) if self.to is not None else 0
        if self.from_ct > self.to_ct > 1:
            raise ValueError(
                f'If passing multiple from_ values, you must pass the '
                f'same number of to values. from={from_}, to={to}')

    @staticmethod
    def cast(value, idx: int, rule_iter: list):
        """
        Attempts to convert value to the python type found at
        rule_iter[idx]. It is built off of isnumericplus so that it can
        even convert floats stored as strings into floats.

        Args:
            value: Any object.
            idx: An integer, the index of the from_ key that value was
                pulled with.
            rule_iter: A list or tuple of python types.

        Returns: value, cast as the appropriate python type if
            possible.

        """
        if value is not None:
            type_ = rule_iter[idx]
            _, c = u.isnumericplus(value, '-convert')
            try:
                value = type_(c)
            except ValueError:
                pass
        return value

    @staticmethod
    def camelcase(value, key, rule_iter: list):
        """
        Applies nicely formatted capitalization to a passed value if
        it's a string.

        Args:
            value: Any object.
            key: The key that value came from in its parent
                OrderedDict.
            rule_iter: A list or tuple of keys that the Rule should
                apply camelcase to.

        Returns: The value, amended for the first letter of each word
            to be capitalized if it's a string.

        """
        if value is not None and key in rule_iter:
            if isinstance(value, str):
                x = ' '.join([i.capitalize() for i in value.split(' ')])
                value = x
        return value

    @staticmethod
    def doregex(value, rule_iter: dict):
        """
        Treats each value in the keys of the passed rule_iter as a
        regex pattern and searches for it in value. Returns the value
        paired with the key matched if a match is found.

        Args:
            value: Any object.
            rule_iter: A dictionary containing tuples as keys and a
                value to be used if value is found in the key tuple.

        Returns: The value, or its replacement if found in the
            rule_iter mapping.

        """
        if value is not None:
            for k, v in rule_iter.items():
                for j in k:
                    if re.search(re.compile(j), str(value)):
                        return v
        return value

    @classmethod
    def methods(cls) -> dict:
        """
        Basically just a dictionary of methods that are valid to pass
        Rule as strings during instantiation. It's a class method so
        that it can be used as a reference by the end-user and also
        can be used when instantiating new Rules.

        Returns: A dictionary of method names as strings as the
            corresponding method object.

        """
        method_map = {
            'cast': cls.cast,
            'camelcase': cls.camelcase,
            'doregex': cls.doregex
        }
        return method_map

    @staticmethod
    def _prep_translation(rule_iter: (list, dict) = None) -> (list, dict):
        """
        Ensures that translation dictionaries are set up properly with
        tuples as keys. Passes lists on untouched.

        Args:
            rule_iter: A list, or dictionary.

        Returns: The list or dictionary (with the dictionary
            adjusted to be ready for use in Rule._translate.)

        """
        if isinstance(rule_iter, dict):
            t_rule = dict()
            for k, v in rule_iter.items():
                if k is None:
                    k = (None, )
                elif not isinstance(k, tuple):
                    k = tuple([k])
                t_rule[k] = v
        else:
            t_rule = rule_iter
        return t_rule

    @staticmethod
    def _translate(value, rule_iter: dict):
        """
        A built-in Rule for mapping values found in from_ to new values
        based on a mapping dictionary that has tuples as keys. This is
        Rule's default function for when it is not passed a callable
        function or a string matching one of its alternate built-in
        Rule methods.

        Args:
            value: Any object.
            rule_iter: A dictionary containing tuples as keys and a
                value to be used if value is found in the key tuple.

        Returns: The value, or its replacement if found in the
            rule_iter mapping.

        """
        for k, v in rule_iter.items():
            if value in k:
                return v
        return value

    def __call__(self, data: col.OrderedDict) -> col.OrderedDict:
        """
        Executes the Rule object's func on the passed OrderedDict.

        Args:
            data: An OrderedDict.

        Returns: The OrderedDict modified by the contents of the Rule.

        """
        for i, f in enumerate(self.from_):
            args = inspect.getfullargspec(self.rule).args
            t = self.from_ if not self.translation else self.translation
            kwargs = dict(idx=i, key=f, rule_iter=t)
            r_kwargs = {k: v for k, v in kwargs.items() if k in args}
            v = self.rule(data.get(f), **r_kwargs)
            if self.to_ct > 1 and self.from_ct == 1:
                for t in self.to:
                    data[t] = v
            elif self.to is not None:
                data[self.to[i]] = v
            else:
                data[f] = v
        return data


class Mapping(Element, col.abc.Mapping):
    """
    Provides a quick way for creating many MappingRules from
    a target format template and from explicitly detailed rules.
    """
    def __init__(self, template: (list, tuple), *rules, **maps):
        """
        Args:
            template: A list or tuple of strings, the header of the
                target data format you want a Dataset to be converted
                into.
            rules: An arbitrary number of strings or Rule objects. Used
                for mapping two columns when you want a default value
                if the source data's value is None or need other
                special functionality.
            maps: An arbitrary number of simple mappings in the vein of
                columnA=ColumnB.
        """
        super(Mapping, self).__init__(dict())
        self.template = template
        # Collect complex mappings from rules:
        for value in rules:
            if isinstance(value, tuple) and self.check_template(value[1]):
                to = value[1]
                self._map_to_data(to, self._gen_base_rule(value[0], to))
            elif isinstance(value, Rule) and self.check_template(value.to):
                self._map_to_data(value.to, value)
            else:
                raise ValueError(
                    f'Passed positional args must all be Rule or tuple '
                    f'objects. Invalid value = {value}')
        # Collect simple mappings from maps:
        for k, v in maps.items():
            if self.check_template(v):
                self._map_to_data(v, self._gen_base_rule(k, v))
        # Generate empty rules for unmapped template values:
        for t in self.template:
            if t not in self._data.keys():
                self._map_to_data(t, self._gen_base_rule(None, t))

    def check_template(self, to: (str, tuple)) -> bool:
        """
        Checks incoming to values against the template to ensure only
        values in the template are passed in rules in maps.

        Args:
            to: A string or tuple of to values.

        Returns: A boolean, effectively only returns True since it will
            raise an error if it would return False.

        """
        if isinstance(to, tuple):
            in_template = len(set(to).difference(set(self.template))) == 0
        else:
            in_template = to in self.template
        if not in_template:
            raise ValueError(
                f'All passed rule/map "to" values must be in the '
                f'passed template. Invalid value: {to}')
        else:
            return in_template

    def plan(self) -> dict:
        """
        Collects a user-friendly dictionary showing how the Mapping
        will work on passed OrderedDicts when called. Mostly this is
        to ease testing.

        Returns: A dictionary detailing the contents of the rules in
            self._data.

        """
        result = dict()
        for k, v in self._data.items():
            result[k] = {'from': v.from_[0], 'to': v.to,
                         'default': v.translation[(None,)]}
        return result

    @staticmethod
    def _gen_base_rule(from_: (str, None), to: (str, tuple)) -> Rule:
        """
        Generates a simple mapping Rule. This gets used a lot.

        Args:
            from_: A string or None if the Rule doesn't map anything
                from the source data to the template.
            to: A string or tuple of strings that the Rule should map
                to.

        Returns: The generated Rule.

        """
        return Rule({None: None}, from_, to=to)

    def _map_to_data(self, to: (str, tuple), rule: Rule) -> None:
        """
        Takes a Rule and maps it to a to key in self._data, after
        confirming that the key isn't already in use.

        Args:
            to: A string or tuple of strings, keys in the template.
            rule: A Rule object with one or more of the values in to
                as its to attribute.

        Returns: None

        """
        to = tuple([to]) if isinstance(to, str) else to
        for t in to:
            if t in self._data.keys():
                raise ValueError(
                    f'Only one mapping rule can be created for each '
                    f'key in the template. Duplicate to value={to}')
            self._data[t] = rule

    def __call__(self, row: col.OrderedDict) -> col.OrderedDict:
        """
        Calls the Mapping on a passed OrderedDict and creates a new
        OrderedDict containing all the fields in self.template, in the
        same order, as well as any values from row that map to those
        fields.

        Args:
            row: An OrderedDict.

        Returns: An OrderedDict that matches the format in the
            Mapping's template.

        """
        row = row.copy()
        result = col.OrderedDict()
        for f in self.template:
            result[f] = self._data[f](row).get(f)
        return result

    def __iter__(self):
        return self._data.__iter__()


