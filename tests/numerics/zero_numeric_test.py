import operator as o

import pytest
import pandas as pd
import numpy as np

from tamer.numerics.zero_numeric import ZeroNumeric


class TestZeroNumeric:
    def test_that_it_properly_reinterprets_on_init(self):
        z = ZeroNumeric("'00123")
        assert z.numeric == 123
        assert z.value == "00123"
        z = ZeroNumeric("'''00123")
        assert z.numeric == 123
        assert z.value == "00123"
        z = ZeroNumeric(123)
        assert z.numeric == 123
        assert z.value == "123"
    
    def test_that_it_can_handle_numpy_numeric_objects(self):
        z = ZeroNumeric(np.int64(123))
        assert z.numeric == 123
        assert z.value == "123"
        assert isinstance(z.numeric, int)
        z = ZeroNumeric(np.float64(123.0))
        assert z.numeric == 123.0
        assert z.value == "123.0"
        assert isinstance(z.numeric, float)

    def test_that_it_returns_errors_on_invalid_inputs(self):
        with pytest.raises(ValueError, match="or value. Invalid value=test"):
            ZeroNumeric("test")
        with pytest.raises(ValueError, match="Cannot convert float NaN to ZeroNumeric"):
            ZeroNumeric(np.nan)


class TestZeroNumericOperations:
    @pytest.fixture
    def sample_zn(self):
        return ZeroNumeric("00124")

    def test_that_it_can_handle_equalities(self, sample_zn):
        assert sample_zn == 124
        assert sample_zn == "00124"

    def test_that_str_operator_adds_apostrophe(self, sample_zn):
        assert str(sample_zn) == "'00124"
    
    def test_that_it_can_handle_math(self, sample_zn):
        x = sample_zn + 1
        assert x == 125
        assert isinstance(x, ZeroNumeric)
        assert sample_zn - 1 == 123
        assert sample_zn * 2 == 248
        assert sample_zn / 2 == 62
        assert sample_zn % 2 == 0
        x = sample_zn - 124
        assert x == 0
        assert not isinstance(x, ZeroNumeric)

    def test_that_do_op_works_with_arbitrary_operators(self, sample_zn):
        assert sample_zn._do_op(o.add, 22) == 146
        assert sample_zn._do_op(o.sub, 22) == 102


class TestZeroNumericPad:
    def test_that_it_doesnt_add_zeros_when_pad_isnt_longer_than_length(self):
        assert ZeroNumeric("123").pad(3) == "123"

    def test_that_it_adds_zeros_when_pad_is_longer_than_length(self):
        assert ZeroNumeric("123").pad(6) == "000123"

    def test_that_it_can_handle_zero_numerics_with_numeric_inputs(self):
        assert ZeroNumeric(1).pad(2) == "01"
        assert ZeroNumeric(np.int64(21)).pad(3) == "021"


class TestZeroNumericSplitZeros:
    def test_that_it_can_handle_a_simple_split(self):
        assert ZeroNumeric.split_zeros("00123") == ("00", 123)

    def test_that_it_can_handle_splits_with_zeros_in_different_places(self):
        assert ZeroNumeric.split_zeros("00123.0") == ("00", 123.0)
        assert ZeroNumeric.split_zeros("001203") == ("00", 1203)
        assert ZeroNumeric.split_zeros("001203.0") == ("00", 1203.0)

    def test_that_it_can_handle_no_zeros(self):
        assert ZeroNumeric.split_zeros("123") == ("", 123)


class TestZeroNumericCasting:
    def test_cast_as_int(self):
        z = ZeroNumeric("00123.0")
        z = z.to_int()
        assert z == 123
        assert isinstance(z.numeric, int)

    def test_cast_as_float(self):
        z = ZeroNumeric("00123")
        z = z.to_float()
        assert z == 123.0
        assert isinstance(z.numeric, float)

    def test_cast_nan_as_zero_numeric(self):
        assert pd.isna(ZeroNumeric.zn_int(np.nan))
