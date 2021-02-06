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
