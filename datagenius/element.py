import collections as col
import inspect
import re
from abc import ABC
from typing import Callable

import pandas as pd

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


class MatchRule(col.abc.MutableSequence):
    """
    A simple object used by genius.Supplement to control what rules
    it uses for creating and merging chunks of Datasets.
    """
    def __init__(self, *on, conditions: dict = None,
                 thresholds: (float, tuple) = None,
                 block: (str, tuple) = None,
                 inexact: bool = False):
        """

        Args:
            *on: An arbitrary list of
            conditions:
            thresholds:
            block:
        """
        self.on: tuple = on
        c = {None: (None,)} if conditions is None else conditions
        for k, v in c.items():
            c[k] = u.tuplify(v)
        self.conditions: dict = c
        self.thresholds: tuple = u.tuplify(thresholds)
        self.block: tuple = u.tuplify(block)
        self.inexact: bool = inexact
        if self.inexact:
            if self.thresholds is None:
                self.thresholds = tuple([.9 for _ in range(len(self.on))])
            elif len(self.thresholds) != len(self.on):
                raise ValueError(
                    f'If provided, thresholds length must match on '
                    f'length: thresholds={self.thresholds}, on={self.on}')
        self.chunks: list = []

    def insert(self, index: int, x):
        self.chunks.insert(index, x)

    def output(self, *attrs) -> tuple:
        """
        Convenience method for quickly collecting a tuple of attributes
        from MatchRule.

        Args:
            *attrs: An arbitrary number of strings, which must be
                attributes in MatchRule. If no attrs are passed, output
                will just return on and conditions attributes.

        Returns: A tuple of attribute values.

        """
        if len(attrs) == 0:
            return self.on, self.conditions
        else:
            results = [getattr(self, a) for a in attrs]
            return results[0] if len(results) == 1 else tuple(results)

    def __getitem__(self, item: int):
        return self.chunks[item]

    def __setitem__(self, key: int, value: list):
        self.chunks[key] = value

    def __delitem__(self, key: int):
        self.chunks.pop(key)

    def __len__(self):
        return len(self.chunks)
