import pytest

import datagenius.element as e


class TestDataset:
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

        d = e.Dataset(raw)
        d.header = header

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

    def test_update_meta_data(self):
        d = e.Dataset([
            [1, 2, 3],
            [4, 5, 6]
        ])

        d.update_meta_data('test', a=1)
        assert d.meta_data['test']['a'] == 1
        d.update_meta_data('test', b=2, c=3)
        assert d.meta_data['test'] == {
            'a': 1, 'b': 2, 'c': 3
        }


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


