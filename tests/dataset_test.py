import pytest

from datagenius.dataset import Dataset, Parser


class TestParser:
    def test_call(self):
        p = Parser(lambda y: y.append(2))
        x = []
        p(x)
        assert x == [2]

        with pytest.raises(ValueError):
            p = Parser()
            p([])

    def test_setattr(self):
        p = Parser()
        with pytest.raises(ValueError):
            p.func = 'error'
            p.func = lambda x, y: x + y


class TestDataset:
    def test_match(self, simple_data):
        expected = [
            [1, 'Yancy', 'Cordwainer'],
            [2, 'Muhammad', 'El-Kanan'],
            [3, 'Luisa', 'Romero'],
        ]
        d = Dataset(simple_data)
        p = Parser(lambda x: True if len(x[2]) > 5 else False)
        assert d.match(p) == expected

    def test_preprocess(self, simple_data, gaps_totals):
        d = Dataset(simple_data)
        d.preprocess()
        assert d == simple_data

        d = Dataset(gaps_totals)
        expected = [
            ['location', 'region', 'sales'],
            ['Bayside Store', 'Northern', 500],
            ['West Valley Store', 'Northern', 300],
            ['Precioso Store', 'Southern', 1000],
            ['Kalliope Store', 'Southern', 200]
        ]
        d.preprocess()
        assert d == expected
