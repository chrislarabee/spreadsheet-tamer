from collections import OrderedDict

import pandas as pd

from tamer import iterutils as u
from tamer.type_handling import isnumericplus


class TestBroadcastAffix:
    def test_that_it_works_on_any_iterable(self):
        expected = ["x_1", "y_1", "z_1"]
        assert u.broadcast_affix(["x", "y", "z"], "_1") == expected
        result = u.broadcast_affix(pd.Index(["x", "y", "z"]), "_1")
        pd.testing.assert_index_equal(result, pd.Index(expected))
        result = u.broadcast_affix(pd.Series(["x", "y", "z"]), "_1")
        pd.testing.assert_series_equal(result, pd.Series(expected))

    def test_that_it_can_suffix_and_prefix(self):
        x = ["x", "y", "z"]
        assert u.broadcast_affix(x, "1", 0) == ["1x", "1y", "1z"]
        assert u.broadcast_affix(x, "1", -1) == ["x1", "y1", "z1"]


class TestBroadcastType:
    def test_that_it_works_on_any_iterable(self):
        expected = [1, 2, 3]
        assert u.broadcast_type(["1", "2", "3"], int) == expected
        result = u.broadcast_type(pd.Series(["1", "2", "3"]), int)
        pd.testing.assert_series_equal(result, pd.Series(expected))

    def test_special_isnumericplus_functionality(self):
        assert u.broadcast_type(["1", "0.5", "2"], isnumericplus) == [1, 0.5, 2]


class TestCollectByKeys:
    def test_that_it_works_with_dict_and_ordered_dict(self):
        x = u.collect_by_keys({"a": 1, "b": 2, "c": 3, "d": 4}, "a", "c")
        assert x == {"a": 1, "c": 3}
        assert isinstance(x, dict) and not isinstance(x, OrderedDict)
        x = u.collect_by_keys(OrderedDict(e=5, f=6, g=7), "e", "f")
        assert x == OrderedDict(e=5, f=6)
        # OrderedDict IS an an instance of dict, so we must directly test against
        # type here.
        assert type(x) == OrderedDict and type(x) != dict


class TestWithinPlus:
    def test_that_it_works_with_a_single_value(self):
        assert u.withinplus([1, 2, 3], 1)

    def test_that_it_works_with_multiple_values(self):
        assert u.withinplus([1, 2, 3], 1, 4)
        assert not u.withinplus([1, 2, 3], 4, 5)
    
    def test_that_it_works_with_single_regex(self):
        assert u.withinplus(["xyz", "a23"], r"[a-z]\d+")
        assert not u.withinplus(["xyz", "a23"], r"[a-z]\d[a-z]")

    def test_that_it_works_with_multiple_regex(self):
        patterns = (r"[a-z]\d+", r"[a-z]\d[a-z]")
        assert u.withinplus(["xyz", "a2a"], *patterns)
        assert not u.withinplus(["xyz", "abc"], *patterns)

    def test_that_regex_works_with_pandas_index(self):
        pattern = r"[Uu]nnamed:*[ _]\d"
        assert u.withinplus(pd.Index(["unnamed_0", "unnamed_1"]), pattern)
        assert u.withinplus(pd.Index(["Unnamed: 0", "Unnamed: 1"]), pattern)
        assert u.withinplus(pd.Index(["Unnamed:_0", "Unnamed:_1"]), pattern)