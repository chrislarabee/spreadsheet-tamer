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


class TestMatchRule:
    def test_basics(self):
        mr = e.MatchRule('a', 'b', 'c', inexact=True)
        # Ensures thresholds is subscriptable:
        assert mr.thresholds[0] == .9

    def test_output(self):
        mr = e.MatchRule('a', 'b', 'c', conditions={'c': 'x'})
        assert mr.output() == (('a', 'b', 'c'), {'c': ('x',)})
        assert mr.output('on', 'thresholds') == (('a', 'b', 'c'), None)
        assert mr.output('on') == ('a', 'b', 'c')
