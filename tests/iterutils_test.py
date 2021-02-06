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
