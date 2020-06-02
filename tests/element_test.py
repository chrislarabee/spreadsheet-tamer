import os
from collections import OrderedDict as od
import statistics

import pytest

import datagenius.element as e
from datagenius.io import odbc


class TestMetaData:
    def test_update_and_clear(self):
        md = e.MetaData()

        md.update('test', a=1)
        assert md['test']['a'] == 1
        md.update('test', b=2, c=3)
        assert md['test'] == {
            'a': 1, 'b': 2, 'c': 3
        }

        md.clear_col_data('test')
        assert md == {}

    def test_calculate(self):
        md = e.MetaData(
            dict(
                a={'null_ct': 5},
                b={'null_ct': 7},
                c={'null_ct': 3},
            )
        )
        assert md.calculate(
            sum,
            'null_ct'
        ) == 15

        assert md.calculate(
            statistics.mean,
            'null_ct',
            'avg_null_ct'
        ) == 5
        assert md.avg_null_ct == 5

    def test_check_key(self):
        md = e.MetaData(dict(
            a={'w': True, 'x': 1},
            b={'w': False, 'x': 2},
            c={'w': True, 'x': 3}
        ))
        assert md.check_key('x')
        assert not md.check_key('y')
        # Ensure boolean values still evaluate properly:
        assert md.check_key('w')

    def test_concat_header(self):
        md = e.MetaData()
        md.header = ['a', 'b', 'c']
        assert md.concat_header(['c', 'd']) == ['a', 'b', 'c', 'd']

    def test_update_attr(self):
        md = e.MetaData()
        md.update_attr('test_list', 1, list)
        assert md.test_list == [1]
        md.update_attr('test_list', 2)
        assert md.test_list == [1, 2]
        md.update_attr('test_dict', {'a': 1}, dict)
        assert md.test_dict == {'a': 1}
        md.update_attr('test_dict', {'b': 2})
        assert md.test_dict == {'a': 1, 'b': 2}
        md.update_attr('test_odict', od(c=3), od)
        assert md.test_odict == od(c=3)
        md.update_attr('test_odict', od(d=4))
        assert md.test_odict == od(c=3, d=4)
        md.update_attr('test_other', 1)
        assert md.test_other == 1


class TestDataset:
    def test_copy(self, customers):
        d = e.Dataset(customers[1], customers[0])

        d2 = d.copy()
        assert d2 != d
        assert d2.meta_data != d.meta_data

    def test_transpose(self):
        data = [
            [1, 2, 3],
            [4, 5, 6],
            [7, 8, 9]
        ]
        d = e.Dataset(data)
        assert d.data_orientation == 'row'
        d.transpose('column')
        assert d == [
            [1, 4, 7],
            [2, 5, 8],
            [3, 6, 9]
        ]
        assert d.data_orientation == 'column'
        d.transpose('set')
        assert d == data
        assert d.data_orientation == 'row'

    def test_from_file(self, simple_data):
        d = e.Dataset.from_file('tests/samples/csv/simple.csv')
        assert isinstance(d, e.Dataset)
        assert d == simple_data()

        d = e.Dataset.from_file('tests/samples/excel/simple.xlsx')
        assert isinstance(d, e.Dataset)
        assert d == simple_data(int)

    def test_remove(self, simple_data):
        d = e.Dataset(simple_data())
        d.remove(0)
        assert d == [
            ['1', 'Yancy', 'Cordwainer', '00025'],
            ['2', 'Muhammad', 'El-Kanan', '00076'],
            ['3', 'Luisa', 'Romero', '00123'],
            ['4', 'Semaj', 'Soto', '01234']
        ]

        d.remove(['4', 'Semaj', 'Soto', '01234'])
        assert d == [
            ['1', 'Yancy', 'Cordwainer', '00025'],
            ['2', 'Muhammad', 'El-Kanan', '00076'],
            ['3', 'Luisa', 'Romero', '00123']
        ]

        with pytest.raises(ValueError,
                           match='can only take int or list'):
            d.remove('bad input')

    def test_format_changes(self):
        raw = [
            [1, 2, 3],
            [4, 5, 6],
            [7, 8, 9]
        ]
        header = ['a', 'b', 'c']
        expected = [
            {'a': 1, 'b': 2, 'c': 3},
            {'a': 4, 'b': 5, 'c': 6},
            {'a': 7, 'b': 8, 'c': 9},
        ]

        d = e.Dataset(raw, header=header)

        assert d.to_dicts() == expected
        assert d.to_lists() == raw
        assert d.to_format('dicts')
        assert d == expected
        assert d.to_format('lists')
        assert d == raw

    def test_getitem(self):
        d = e.Dataset([
            [1, 2, 3],
            [4, 5, 6]
        ])

        assert d[0] == [1, 2, 3]

    def test_index(self):
        d = e.Dataset([
            [1, 2, 3],
            [4, 5, 6]
        ])

        assert d.index([1, 2, 3]) == 0

    def test_comparison(self):
        d = e.Dataset([
            [1, 2, 3],
            [4, 5, 6]
        ])

        assert d == [[1, 2, 3], [4, 5, 6]]
        assert d != [[0, 0, 0], [9, 9, 9]]

    def test_to_file_sqlite(self, sales):
        d = e.Dataset(sales[1], sales[0])
        d.rejects = [
            ['Sales by Location Report', None, None],
            ['Grouping: Region', 43956.0, None],
            [None, None, 800],
            [None, None, 1200]
        ]
        d.to_format('dicts')
        # No meta_data should raise an error:
        with pytest.raises(ValueError, match='Discrepancy between meta'):
            d.to_file('tests/samples', 'sales')
        # Now add meta_data:
        d.meta_data.update('location', probable_type='uncertain')
        d.meta_data.update('region', probable_type='string')
        d.meta_data.update('sales', probable_type='integer')
        o = odbc.ODBConnector()
        d.to_file('tests/samples', 'sales', db_conn=o, db_name='element_test')
        d2 = e.Dataset(o.select('sales'))
        assert d2._data == d._data

        # Check meta data tables:
        assert e.Dataset(o.select('sales_col_meta_data')) == [
            od(column='location', probable_type='uncertain'),
            od(column='region', probable_type='string'),
            od(column='sales', probable_type='integer')
        ]
        assert e.Dataset(o.select('sales_dset_meta_data')) == [
            od(feature='Number of columns', value='3'),
            od(feature='Number of rows', value='4'),
            od(feature='Number of rejected rows', value='4'),
            od(feature='Number of values in rejected rows', value='5'),
            od(feature='Number of strings cleared of whitespace', value='0')
        ]

        # Check rejects table:
        assert e.Dataset(o.select('sales_rejects')) == [
            od(location='Sales by Location Report', region=None, sales=None),
            od(location='Grouping: Region', region='43956.0', sales=None),
            od(location=None, region=None, sales='800'),
            od(location=None, region=None, sales='1200'),
        ]

    def test_to_file_csv(self, customers, simple_data):
        p = 'tests/samples/customers.csv'
        if os.path.exists(p):
            os.remove(p)
        d = e.Dataset(customers[1], customers[0])
        d.to_format('dicts')
        d.to_file('tests/samples', 'customers', to='csv')
        d2 = e.Dataset.from_file(p)
        assert d2 == simple_data()

    def test_package_rejects(self):
        d = e.Dataset([
            [1, 2, 3],
            [4, 5, 6]
        ])
        d.rejects = [
            ['Integers', None, None],
            [None, None, 9]
        ]
        assert d.package_rejects() == (
            [
                od({'0': 'Integers', '1': None, '2': None}),
                od({'0': None, '1': None, '2': 9})
            ],
            {'0': str, '1': str, '2': str}
        )


class TestRule:
    def test_init(self):
        r = e.Rule('test', lambda x: x + 1)
        assert r.from_ == ('test',)
        assert r.to is None

        with pytest.raises(
                ValueError,
                match='rule must be a callable object'):
            r = e.Rule('test', 'bad_func')

        with pytest.raises(
                ValueError,
                match='If passing multiple from_ values'):
            r = e.Rule(('a', 'test', '!'), lambda x: x + 1, to=('odd', 'to'))

    def test_call(self):
        r = e.Rule(('a', 'b'), lambda x: x + 1, to=('c', 'd'))
        assert r(od(a=1, b=3)) == od(a=1, b=3, c=2, d=4)

        r = e.Rule('a', lambda x: x * 10, to=('b', 'c'))
        assert r(od(a=1)) == od(a=1, b=10, c=10)

    def test_translation_and_mapping_functionality(self):
        r = e.Rule('test', dict(a='b', c='d'))
        assert r._translation == {('a',): 'b', ('c',): 'd'}
        assert r(od(test='a')) == od(test='b')

        r = e.Rule('test', dict(x='y', v='w'), to='output')
        assert r(od(test='x')) == od(test='x', output='y')

        r = e.Rule('test', {'bird': 'word'})
        assert r(od(test='bird')) == od(test='word')

        r = e.Rule('test', {None: 1234})
        assert r(od(test=None)) == od(test=1234)
        assert r(od(test=90)) == od(test=90)

        r = e.Rule('test', {('x', 'y'): 'z'}, to='output')
        assert r(od(test='x')) == od(test='x', output='z')
        assert r(od(test='y')) == od(test='y', output='z')

    def test_cast(self):
        r = e.Rule(('a', 'b', 'c'), 'cast', (float, int, str))
        assert r(od(a='1', b='2.0', c='test')) == od(a=1.0, b=2, c='test')
        assert r(od(a='1..23', b='1.23', c=None)) == od(a=1.23, b=1, c=None)

    def test_camelcase(self):
        d = od(a='ALL CAPS', b='no caps', c='A mix OF Both')
        r = e.Rule(('a', 'b', 'c'), 'camelcase')
        assert r(d) == od(a='All Caps', b='No Caps', c='A Mix Of Both')


class TestMapping:
    def test_basics(self):
        t = ['w', 'x', 'y', 'z']
        expected = {
            'a': {'from': 'a', 'to': 'w', 'default': None},
            'b': {'from': 'b', 'to': 'x', 'default': None},
            'c': {'from': 'c', 'to': 'z', 'default': 1},
            'd': {'from': 'd', 'to': 'y', 'default': None}
        }

        m = e.Mapping(
            t,
            e.Rule('c', {None: 1}, to='z'),
            a='w',
            b='x',
            d='y'
        )
        assert m.plan() == expected

        with pytest.raises(
                ValueError, match='All passed rule/map to values must'):
            m = e.Mapping(t, a='omega')
            m = e.Mapping(t, e.Rule('a', {None: None}, 'omega'))

        with pytest.raises(
                ValueError, match='Passed positional args must all be'):
            m = e.Mapping(t, 'not a rule')

        expected = od(w=7, x=8, y=9, z=1)
        assert m(od(a=7, b=8, c=None, d=9)) == expected

        expected = od(w=1, x=2, y=None, z=1)
        assert m(od(a=1, b=2)) == expected

