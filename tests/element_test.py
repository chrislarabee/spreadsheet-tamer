import operator as o

import pytest
import pandas as pd
from numpy import nan

import datagenius.element as e


class TestZeroNumeric:
    def test_basics(self):
        z = e.ZeroNumeric("'00123")
        assert z.numeric == 123
        assert z.value == "00123"
        z = e.ZeroNumeric("'''00123")
        assert z.numeric == 123
        assert z.value == "00123"
        z = e.ZeroNumeric(123)
        assert z.numeric == 123
        assert z.value == '123'
        z = e.ZeroNumeric('00124')
        assert z.numeric == 124
        assert z.value == '00124'
        assert z.zeros == '00'
        assert z == 124
        assert z == '00124'
        assert str(z) == "'00124"
        x = z + 1
        assert x == 125
        assert isinstance(x, e.ZeroNumeric)
        assert z - 1 == 123
        assert z * 2 == 248
        assert z / 2 == 62
        assert z % 2 == 0
        x = z - 124
        assert x == 0
        assert not isinstance(x, e.ZeroNumeric)

        with pytest.raises(
                ValueError, match='or value. Invalid value=test'):
            e.ZeroNumeric('test')

        with pytest.raises(
                ValueError,
                match='Cannot convert float NaN to ZeroNumeric'):
            e.ZeroNumeric(nan)

    def test_pad(self):
        assert e.ZeroNumeric('123').pad(3) == '123'
        assert e.ZeroNumeric('123').pad(6) == '000123'

    def test_split_zeros(self):
        assert e.ZeroNumeric.split_zeros('00123') == ('00', 123)
        assert e.ZeroNumeric.split_zeros('00123.0') == ('00', 123.0)
        assert e.ZeroNumeric.split_zeros('001203') == ('00', 1203)
        assert e.ZeroNumeric.split_zeros('001203.0') == ('00', 1203.0)
        assert e.ZeroNumeric.split_zeros('123') == ('', 123)

    def test_do_op(self):
        assert e.ZeroNumeric('00123').do_op(o.add, 22) == 145
        assert e.ZeroNumeric('00123').do_op(o.sub, 22) == 101

    def test_casting(self):
        z = e.ZeroNumeric('00123.0')
        z = z.to_int()
        assert z == 123
        assert isinstance(z.numeric, int)
        z = z.to_float()
        assert z == 123.0
        assert isinstance(z.numeric, float)

        assert pd.isna(e.ZeroNumeric.zn_int(nan))
