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
        p = pa.parser(lambda x: (x if len(x[2]) > 5 else None))
        assert d.loop(p) == expected

        p = pa.parser(lambda x: 1 if len(x[2]) > 5 else 0)
        expected = [0, 1, 1, 1, 0]
        assert d.loop(p) == expected

    def test_getitem(self):
        d = Dataset([
            [1, 2, 3],
            [4, 5, 6]
        ])

        assert d[0] == [1, 2, 3]
