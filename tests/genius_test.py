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
    def test_basic_go(self, customers, sales, simple_data, gaps,
                      gaps_totals):
        p = ge.Preprocess()
        d = Dataset(simple_data())
        r = p.go(d)
        assert r == d
        assert r == customers[1]
        assert d.header == customers[0]

        d = Dataset(gaps)
        r = p.go(d, overwrite=False)
        assert r == customers[1]
        assert r != d
        assert d.header is None
        assert r.header == customers[0]

        d = Dataset(gaps_totals)
        r = p.go(d)
        assert r == sales[1]
        assert r.header == sales[0]

    def test_custom_go(self):
        # Test custom preprocess step and header_func:
        pr = pa.parser(
            lambda x: [str(x[0]), *x[1:]],
            requires_header=False
        )
        hf = pa.parser(
            lambda x: x if x[0] == 'odd' else None,
            requires_header=False,
            breaks_loop=True
        )
        d = Dataset([
            ['', '', ''],
            ['odd', 1, 'header'],
            [1, 2, 3],
            [None, None, None],
            [4, 5, 6]
        ])

        assert ge.Preprocess(pr).go(d, header_func=hf) == [
            ['1', 2, 3],
            ['4', 5, 6]
        ]
        assert d.header == ['odd', 1, 'header']

        # Test manual_header:
        d = Dataset([
            [1, 2, 3],
            [4, 5, 6]
        ])

        assert ge.Preprocess().go(
            d,
            manual_header=['a', 'b', 'c']) == [
            [1, 2, 3],
            [4, 5, 6]
        ]
        assert d.header == ['a', 'b', 'c']
