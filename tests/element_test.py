import os
from collections import OrderedDict as od
import statistics

import pytest
import pandas as pd

import datagenius.element as e
from datagenius.io import odbc


class TestMetaData:
    def test_basics(self, customers):
        md = e.MetaData()
        assert md.parent is None

        d = e.Dataset(**customers())
        md = e.MetaData(d)
        assert md.header_idx == 0
        assert md.init_col_ct == 4
        assert md.init_row_ct == 4

        d = e.Dataset.from_file('tests/samples/csv/gaps.csv')
        assert d.meta_data.header_idx is None

        d = e.Dataset([[1, 2, 3], [4, 5, 6]])
        assert d.meta_data.header_idx is None

    def test_reject_ct(self, products):
        d = e.Dataset(**products)
        d.dropna(thresh=7, inplace=True)
        assert d.shape[0] == 2
        assert d.meta_data.init_row_ct == 3
        assert d.meta_data.reject_ct == 1

    # def test_update_and_clear(self):
    #     md = e.MetaData()
    #
    #     md.update('test', a=1)
    #     assert md['test']['a'] == 1
    #     md.update('test', b=2, c=3)
    #     assert md['test'] == {
    #         'a': 1, 'b': 2, 'c': 3
    #     }
    #
    #     md.clear_col_data('test')
    #     assert md == {}
    #
    # def test_calculate(self):
    #     md = e.MetaData(
    #         dict(
    #             a={'null_ct': 5},
    #             b={'null_ct': 7},
    #             c={'null_ct': 3},
    #         )
    #     )
    #     assert md.calculate(
    #         sum,
    #         'null_ct'
    #     ) == 15
    #
    #     assert md.calculate(
    #         statistics.mean,
    #         'null_ct',
    #         'avg_null_ct'
    #     ) == 5
    #     assert md.avg_null_ct == 5
    #
    # def test_check_key(self):
    #     md = e.MetaData(dict(
    #         a={'w': True, 'x': 1},
    #         b={'w': False, 'x': 2},
    #         c={'w': True, 'x': 3}
    #     ))
    #     assert md.check_key('x')
    #     assert not md.check_key('y')
    #     # Ensure boolean values still evaluate properly:
    #     assert md.check_key('w')
    #
    # def test_concat_header(self):
    #     md = e.MetaData()
    #     md.header = ['a', 'b', 'c']
    #     assert md.concat_header(['c', 'd']) == ['a', 'b', 'c', 'd']
    #
    # def test_update_attr(self):
    #     md = e.MetaData()
    #     md.update_attr('test_list', 1, list)
    #     assert md.test_list == [1]
    #     md.update_attr('test_list', 2)
    #     assert md.test_list == [1, 2]
    #     md.update_attr('test_dict', {'a': 1}, dict)
    #     assert md.test_dict == {'a': 1}
    #     md.update_attr('test_dict', {'b': 2})
    #     assert md.test_dict == {'a': 1, 'b': 2}
    #     md.update_attr('test_odict', od(c=3), od)
    #     assert md.test_odict == od(c=3)
    #     md.update_attr('test_odict', od(d=4))
    #     assert md.test_odict == od(c=3, d=4)
    #     md.update_attr('test_other', 1)
    #     assert md.test_other == 1


class TestDataset:
    def test_basics(self):
        d = e.Dataset([dict(a=1, b=2, c=3), dict(a=4, b=5, c=6)])
        md = d.meta_data
        assert isinstance(d, pd.DataFrame)
        d2 = d[['a', 'b']]
        assert isinstance(d2, e.Dataset)
        assert d2.meta_data == md

    def test_from_file(self, customers):
        d = e.Dataset.from_file(
            'tests/samples/csv/simple.csv')
        pd.testing.assert_frame_equal(
            d, pd.DataFrame(**customers())
        )
        assert isinstance(d, e.Dataset)

        # Ensure null rows are being dropped from csv:
        d = e.Dataset.from_file(
            'tests/samples/csv/gaps.csv')
        assert d.shape == (5, 4)
        assert isinstance(d, e.Dataset)

        d = e.Dataset.from_file(
            'tests/samples/excel/simple.xlsx')
        pd.testing.assert_frame_equal(
            d, e.Dataset(**customers(int))
        )
        assert isinstance(d, e.Dataset)

        # Ensure null rows are being dropped from excel:
        d = e.Dataset.from_file(
            'tests/samples/excel/gaps_totals.xlsx')
        assert d.shape == (8, 3)
        assert isinstance(d, e.Dataset)

        d = e.Dataset.from_file(
            'tests/samples/sqlite', table='customers', db_name='read_testing')
        pd.testing.assert_frame_equal(
            d, e.Dataset(**customers())
        )
        assert isinstance(d, e.Dataset)

    def test_to_from_sqlite(self, sales):
        d = e.Dataset(**sales)
        o = odbc.ODBConnector()
        d.to_sqlite(
            'tests/samples', 'sales', db_conn=o, db_name='element_test')
        d2 = e.Dataset.from_file(
            'tests/samples/', table='sales', db_conn=o, db_name='element_test')
        pd.testing.assert_frame_equal(d, d2)

        # d.rejects = [
        #     ['Sales by Location Report', None, None],
        #     ['Grouping: Region', 43956.0, None],
        #     [None, None, 800],
        #     [None, None, 1200]
        # ]
        # d.meta_data.update('location', probable_type='uncertain')
        # d.meta_data.update('region', probable_type='string')
        # d.meta_data.update('sales', probable_type='integer')
    #
    #     # Check meta data tables:
    #     assert e.Dataset(o.select('sales_col_meta_data')) == [
    #         od(column='location', probable_type='uncertain'),
    #         od(column='region', probable_type='string'),
    #         od(column='sales', probable_type='integer')
    #     ]
    #     assert e.Dataset(o.select('sales_dset_meta_data')) == [
    #         od(feature='Number of columns', value='3'),
    #         od(feature='Number of rows', value='4'),
    #         od(feature='Number of rejected rows', value='4'),
    #         od(feature='Number of values in rejected rows', value='5'),
    #         od(feature='Number of strings cleared of whitespace', value='0')
    #     ]
    #
    #     # Check rejects table:
    #     assert e.Dataset(o.select('sales_rejects')) == [
    #         od(location='Sales by Location Report', region=None, sales=None),
    #         od(location='Grouping: Region', region='43956.0', sales=None),
    #         od(location=None, region=None, sales='800'),
    #         od(location=None, region=None, sales='1200'),
    #     ]
    #
    # def test_package_rejects(self):
    #     d = e.Dataset([
    #         [1, 2, 3],
    #         [4, 5, 6]
    #     ])
    #     d.rejects = [
    #         ['Integers', None, None],
    #         [None, None, 9]
    #     ]
    #     assert d.package_rejects() == (
    #         [
    #             od({'0': 'Integers', '1': None, '2': None}),
    #             od({'0': None, '1': None, '2': 9})
    #         ],
    #         {'0': str, '1': str, '2': str}
    #     )


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
