import pytest

from datagenius.dataset import Dataset
from datagenius import parsers as pa


class TestDataset:
    def test_loop(self, simple_data):
        expected = [
            ['1', 'Yancy', 'Cordwainer', '00025'],
            ['2', 'Muhammad', 'El-Kanan', '00076'],
            ['3', 'Luisa', 'Romero', '00123'],
        ]
        d = Dataset(simple_data())
        p = pa.parser(lambda x: (x if len(x[2]) > 5 else None),
                      requires_header=False)
        assert d.loop(p) == expected

        p = pa.parser(lambda x: 1 if len(x[2]) > 5 else 0,
                      requires_header=False)
        expected = [0, 1, 1, 1, 0]
        assert d.loop(p) == expected

        with pytest.raises(ValueError,
                           match='decorated as parsers'):
            d.loop(lambda x: x + 1)

        with pytest.raises(ValueError,
                           match='requires a header'):
            d.loop(
                pa.parser(lambda x: x)
            )

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
