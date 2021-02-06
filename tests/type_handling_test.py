import pandas as pd
from numpy import nan

from tamer import type_handling as t
from tamer.numerics.zero_numeric import ZeroNumeric


class TestConvertPlus:
    def test_that_it_can_convert_various_types(self):
        assert t.convertplus(123, target_type=str) == "123"
        assert isinstance(t.convertplus("00123", ZeroNumeric), ZeroNumeric)
        assert t.convertplus(1.23, int) == 1
        assert isinstance(t.convertplus(1234.0, int), int)
        assert t.convertplus([1, 2, 3], str) == "[1, 2, 3]"
        assert t.convertplus(dict(a=1, b=2, c=3), str) == "{'a': 1, 'b': 2, 'c': 3}"
        assert t.convertplus("0", float) == 0.0
        assert t.convertplus("1", float) == 1.0

    def test_that_it_can_handle_nans(self):
        assert pd.isna(t.convertplus(nan, int))

    def test_that_it_can_be_used_in_pandas_apply(self):
        s = pd.Series([1, nan, "2", 1.2])
        expected = pd.Series([1, nan, 2, 1])
        result = s.apply(t.convertplus, target_type=int)
        pd.testing.assert_series_equal(expected, result)

    def test_that_it_can_handle_weird_strings(self):
        assert t.convertplus("1..23", float) == 1.23


class TestIsNumericPlus:
    def test_that_it_can_handle_an_integer(self):
        x = 1
        assert t.isnumericplus(x) == True
        assert t.isnumericplus(x, return_type=True) == (True, int)

    def test_that_it_can_handle_a_float(self):
        x = 1.5
        assert t.isnumericplus(x) == True
        assert t.isnumericplus(x, return_type=True) == (True, float)

    def test_that_it_can_handle_an_integer_stored_as_string(self):
        x = "1"
        assert t.isnumericplus(x) == True
        assert t.isnumericplus(x, return_type=True) == (True, int)

    def test_that_it_can_handle_a_float_stored_as_string(self):
        x = "1.5"
        assert t.isnumericplus(x) == True
        assert t.isnumericplus(x, return_type=True) == (True, float)

    def test_that_it_can_handle_a_zero_numeric(self):
        x = ZeroNumeric("000123")
        assert t.isnumericplus(x) == True
        assert t.isnumericplus(x, return_type=True) == (True, ZeroNumeric)

    def test_that_it_can_handle_a_zero_numeric_stored_as_string(self):
        x = "000123"
        assert t.isnumericplus(x) == True
        assert t.isnumericplus(x, return_type=True) == (True, ZeroNumeric)

    def test_that_it_can_handle_non_numeric_objects(self):
        x = "abc"
        assert t.isnumericplus(x) == False
        assert t.isnumericplus(x, return_type=True) == (False, str)
        x = [1, 2, 3]
        assert t.isnumericplus(x) == False
        assert t.isnumericplus(x, return_type=True) == (False, list)


class TestTypePlus:
    def test_that_it_can_handle_non_nans(self):
        assert t.type_plus(1) == int
        assert t.type_plus("test") == str
        assert t.type_plus(2.1) == float

    def test_that_it_can_handle_nans(self):
        assert pd.isna(t.type_plus(nan))

    def test_that_it_can_be_used_in_pandas_apply(self):
        s = pd.Series([1, nan, "2", 1.2])
        expected = pd.Series([int, nan, str, float])
        result = s.apply(t.type_plus)
        pd.testing.assert_series_equal(expected, result)
