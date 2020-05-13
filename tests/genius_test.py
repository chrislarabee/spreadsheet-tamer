import pytest

from datagenius.dataset import Dataset
import datagenius.genius as ge
import datagenius.parsers as pa


class TestGenius:
    def test_loop(self, simple_data):
        expected = [
            ['1', 'Yancy', 'Cordwainer', '00025'],
            ['2', 'Muhammad', 'El-Kanan', '00076'],
            ['3', 'Luisa', 'Romero', '00123'],
        ]
        d = Dataset(simple_data())
        p = pa.parser(lambda x: (x if len(x[2]) > 5 else None),
                      requires_header=False)
        assert ge.Genius.loop(d, p) == expected

        p = pa.parser(lambda x: 1 if len(x[2]) > 5 else 0,
                      requires_header=False)
        expected = [0, 1, 1, 1, 0]
        assert ge.Genius.loop(d, p) == expected

        d = Dataset([
            [1, 2, 3],
            [2, 3, 4],
            [3, 4, 5]
        ])

        p = pa.parser(lambda x: x if x[0] > 1 else None,
                      requires_header=False, breaks_loop=True)
        assert ge.Genius.loop(d, p) == [[2, 3, 4]]

        with pytest.raises(ValueError,
                           match='decorated as parsers'):
            ge.Genius.loop(d, lambda x: x + 1)

        with pytest.raises(ValueError,
                           match='requires a header'):
            ge.Genius.loop(d, pa.parser(lambda x: x))


class TestPreprocess:
    def test_go(self, simple_data, gaps, gaps_totals):
        p = ge.Preprocess()
        d = Dataset(simple_data())
        assert p.go(d) == simple_data()
        assert p.go(d) == d
        assert d.header == ['id', 'fname', 'lname', 'foreign_key']

        d = Dataset(gaps)
        assert p.go(d) == simple_data()
        assert p.go(d) == d
        assert d.header == ['id', 'fname', 'lname', 'foreign_key']

        d = Dataset(gaps_totals)

