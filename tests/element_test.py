import pytest

import datagenius.element as e


class TestZeroNumeric:
    def test_basics(self):
        assert e.ZeroNumeric('00123') == '00123'
        assert e.ZeroNumeric('00123') != '123'
        assert str(e.ZeroNumeric('00123')) == "'00123"

        with pytest.raises(
                ValueError, match='numeric value. Invalid value=test'):
            e.ZeroNumeric('test')

    def test_pad(self):
        assert e.ZeroNumeric('123').pad(3) == '123'
        assert e.ZeroNumeric('123').pad(6) == '000123'
