import pytest

from datagenius.dataset import Dataset


class TestDataset:
    def test_from_file(self, simple_data):
        d = Dataset.from_file('tests/samples/csv/simple.csv')
        assert isinstance(d, Dataset)
        assert d == simple_data()

        d = Dataset.from_file('tests/samples/excel/simple.xlsx')
        assert isinstance(d, Dataset)
        assert d == simple_data(int)

    def test_remove(self, simple_data):
        d = Dataset(simple_data())
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

        d = Dataset(raw)
        d.header = header

        assert d.to_dicts() == expected
        assert d.to_lists() == raw

    def test_getitem(self):
        d = Dataset([
            [1, 2, 3],
            [4, 5, 6]
        ])

        assert d[0] == [1, 2, 3]

    def test_index(self):
        d = Dataset([
            [1, 2, 3],
            [4, 5, 6]
        ])

        assert d.index([1, 2, 3]) == 0
