from collections import OrderedDict as od

import pytest
import pandas as pd
from numpy import nan

import datagenius.element as e


class TestCleaningGuide:
    def test_basics(self):
        cg = e.CleaningGuide(
            ('a', 'x'),
            (('b', 'c'), 'y'),
            d='z'
        )
        assert cg('a') == 'x'
        assert cg('b') == 'y'
        assert cg('c') == 'y'
        assert cg('d') == 'z'
        assert cg('e') == 'e'

    def test_convert(self):
        cg = e.CleaningGuide.convert(
            e.CleaningGuide(
                ('a', 'x'),
                (('b', 'c'), 'y'),
                d='z'
            )
        )
        assert cg('a') == 'x'
        assert cg('b') == 'y'
        assert cg('c') == 'y'
        assert cg('d') == 'z'
        assert cg('e') == 'e'

        cg = e.CleaningGuide.convert(
            dict(a='x', b='y', c='z')
        )
        assert cg('a') == 'x'
        assert cg('b') == 'y'
        assert cg('c') == 'z'
        assert cg('e') == 'e'

        with pytest.raises(
                ValueError,
                match="Invalid object=test, type=<class 'str'>"):
            cg = e.CleaningGuide.convert('test')


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


class TestRule:
    def test_init(self):
        r = e.Rule(lambda x: x + 1, 'test')
        assert r.from_ == ('test',)
        assert r.to is None
        assert r.translation is None

        with pytest.raises(
                ValueError,
                match='If passing multiple from_ values'):
            r = e.Rule(lambda x: x + 1, ('a', 'test', '!'), to=('odd', 'to'))

    def test_call(self):
        r = e.Rule(lambda x: x + 1, ('a', 'b'), to=('c', 'd'))
        assert r(od(a=1, b=3)) == od(a=1, b=3, c=2, d=4)

        r = e.Rule(lambda x: x * 10, 'a', to=('b', 'c'))
        assert r(od(a=1)) == od(a=1, b=10, c=10)

    def test_translation_and_mapping_functionality(self):
        r = e.Rule(dict(a='b', c='d'), 'test')
        assert r.translation == {('a',): 'b', ('c',): 'd'}
        assert r.from_ == ('test',)
        assert r.to is None
        assert r(od(test='a')) == od(test='b')

        r = e.Rule(dict(x='y', v='w'), 'test', to='output')
        assert r(od(test='x')) == od(test='x', output='y')

        r = e.Rule({'bird': 'word'}, 'test')
        assert r(od(test='bird')) == od(test='word')

        r = e.Rule({None: 1234}, 'test')
        assert r(od(test=None)) == od(test=1234)
        assert r(od(test=90)) == od(test=90)

        r = e.Rule({('x', 'y'): 'z'}, 'test', to='output')
        assert r(od(test='x')) == od(test='x', output='z')
        assert r(od(test='y')) == od(test='y', output='z')

    def test_cast(self):
        r = e.Rule('cast', [float, int, str], ('a', 'b', 'c'))
        assert r(od(a='1', b='2.0', c='test')) == od(a=1.0, b=2, c='test')
        assert r(od(a='1..23', b='1.23', c=None)) == od(a=1.23, b=1, c=None)

    def test_camelcase(self):
        d = od(a='ALL CAPS', b='no caps', c='A mix OF Both')
        r = e.Rule('camelcase', ('a', 'b', 'c'))
        assert r(d) == od(a='All Caps', b='No Caps', c='A Mix Of Both')

    def test_doregex(self):
        r = e.Rule('doregex', {r'\d+': 'number'}, 'a', to='b')
        assert r(od(a='1234')) == od(a='1234', b='number')
        assert r(od(a='ytterbium')) == od(a='ytterbium', b='ytterbium')
        assert r(od(a=None)) == od(a=None, b=None)

        r = e.Rule('doregex', {r'.': 'notnull'}, 'a', to='b')
        assert r(od(a=1)) == od(a=1, b='notnull')
        assert r(od(a='foobar')) == od(a='foobar', b='notnull')
        assert r(od(a=None)) == od(a=None, b=None)


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
