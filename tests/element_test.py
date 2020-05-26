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
            od(feature='Number of rejected rows', value='0'),
            od(feature='Number of values in rejected rows', value='0'),
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
                {'0': 'Integers', '1': None, '2': None},
                {'0': None, '1': None, '2': 9}
            ],
            {'0': str, '1': str, '2': str}
        )


class TestMappingRule:
    def test_basics(self):
        mr = e.MappingRule('test_col', 1234)

        assert mr() == ('test_col', 1234)
        assert mr(90) == ('test_col', 90)


class TestMapping:
    def test_init(self):
        t = ['a lot', 'of', 'columns', 'for sure']
        expected = {
            'a lot': {'to': 'some more', 'default': None},
            'columns': {'to': 'cols', 'default': None},
            'for sure': {'to': 'here', 'default': 1},
            'of': {'to': None, 'default': None}
        }

        m = e.Mapping(
            t,
            {'a lot': 'some more',
             'columns': e.MappingRule('cols'),
             'for sure': e.MappingRule('here', 1)
             }
        )

        assert m == expected

        with pytest.raises(
                ValueError,
                match='must be strings or MappingRule'):
            e.Mapping(
                t,
                {'a lot': 1}
            )

        with pytest.raises(
                ValueError,
                match='must be in the passed template'):
            e.Mapping(
                t,
                {'a bad key': 'column'}
            )


