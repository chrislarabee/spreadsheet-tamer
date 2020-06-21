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


class TestMapping:
    def test_check_template(self):
        m = e.Mapping(['w', 'x', 'y', 'z'])

        assert m.check_template('z')
        assert m.check_template(('y', 'z'))

        with pytest.raises(
                ValueError, match='All passed rule/map "to" values must'):
            m.check_template('omega')
            m.check_template(('alpha', 'omega'))

    def test_map_to_data(self):
        m = e.Mapping(['a', 'b', 'c'])
        # Have to remove the auto-generated mapping for a:
        m._data.pop('a')

        r = e.Rule({None: None}, 'x', to='a')
        m._map_to_data('a', r)
        assert m.plan()['a'] == {
            'from': 'x', 'to': ('a',), 'default': None}

        with pytest.raises(
                ValueError, match='Only one mapping rule can be created'):
            m._map_to_data('a', e.Rule({None: 1}, 'y', to='a'))

    def test_basics(self):
        t = ['q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z', 'w 2']
        expected = {
            'q': {'from': None, 'to': ('q',), 'default': None},
            'r': {'from': 'e', 'to': ('r', 's'), 'default': 2},
            's': {'from': 'e', 'to': ('r', 's'), 'default': 2},
            't': {'from': 'f', 'to': ('t', 'u'), 'default': None},
            'u': {'from': 'f', 'to': ('t', 'u'), 'default': None},
            'v': {'from': 'a', 'to': ('v', 'w'), 'default': None},
            'w': {'from': 'a', 'to': ('v', 'w',), 'default': None},
            'x': {'from': 'b', 'to': ('x',), 'default': None},
            'z': {'from': 'c', 'to': ('z',), 'default': 1},
            'y': {'from': 'd', 'to': ('y',), 'default': None},
            'w 2': {'from': 'a 1', 'to': ('w 2',), 'default': None}
        }

        m = e.Mapping(
            t,
            e.Rule({None: 2}, 'e', to=('r', 's')),
            e.Rule({None: 1}, 'c', to='z'),
            ('a 1', 'w 2'),
            ('f', ('t', 'u')),
            a=('v', 'w'),
            b='x',
            d='y'
        )
        assert m.plan() == expected

        with pytest.raises(
                ValueError, match='Passed positional args must all be'):
            m = e.Mapping(t, 'not a rule')

        m.template = ['w', 'x', 'y', 'z']
        expected = od(w=7, x=8, y=9, z=1)
        assert m(od(a=7, b=8, c=None, d=9)) == expected

        expected = od(w=1, x=2, y=None, z=1)
        assert m(od(a=1, b=2)) == expected


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
