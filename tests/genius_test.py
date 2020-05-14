import pytest

from datagenius.element import Dataset
import datagenius.genius as ge


def test_parser():
    # Decorator without arguments:
    @ge.parser
    def f(x):
        return x * 10

    assert f.is_parser
    assert not f.breaks_loop
    assert f.null_val is None

    # Decorator with arguments:
    @ge.parser(breaks_loop=True)
    def g(x):
        return x + 1

    assert g.breaks_loop
    assert g.null_val is None

    # Check set_parser/uses_cache conflict:
    with pytest.raises(ValueError,
                       match='set_parsers cannot use cache'):
        ge.parser(lambda x: x + 1, uses_cache=True, set_parser=True)

    # Sanity check to ensure pre-built parsers work:
    assert not ge.Preprocess.cleanse_gap.breaks_loop

    # Sanity check to ensure lambda function parsers work:
    p = ge.parser(lambda x: x + 1, null_val=0)

    assert p.null_val == 0
    assert p(3) == 4


class TestGenius:
    def test_loop(self, simple_data):
        # Test simple filtering loop:
        expected = [
            ['1', 'Yancy', 'Cordwainer', '00025'],
            ['2', 'Muhammad', 'El-Kanan', '00076'],
            ['3', 'Luisa', 'Romero', '00123'],
        ]
        d = Dataset(simple_data())
        p = ge.parser(lambda x: (x if len(x[2]) > 5 else None),
                      requires_header=False)
        assert ge.Genius.loop(d, p) == expected

        # Test loop that generates new values:
        p = ge.parser(lambda x: 1 if len(x[2]) > 5 else 0,
                      requires_header=False)
        expected = [0, 1, 1, 1, 0]
        assert ge.Genius.loop(d, p) == expected

        # Test breaks_loop
        d = Dataset([
            [1, 2, 3],
            [2, 3, 4],
            [3, 4, 5]
        ])

        p = ge.parser(lambda x: x if x[0] > 1 else None,
                      requires_header=False, breaks_loop=True)
        assert ge.Genius.loop(d, p) == [[2, 3, 4]]

        # Test args:
        @ge.parser(requires_header=False, takes_args=True)
        def arg_parser(x, y):
            return x if x[0] > y else None
        assert ge.Genius.loop(
            d, arg_parser, parser_args={
                'arg_parser': {'y': 2}}
        ) == [[3, 4, 5]]

        # Test condition:
        @ge.parser(requires_header=False, condition='0 <= 2')
        def conditional_parser(x):
            x.append(0)
            return x
        assert ge.Genius.loop(
            d, conditional_parser
        ) == [
            [1, 2, 3, 0],
            [2, 3, 4, 0],
            [3, 4, 5]
        ]

        with pytest.raises(ValueError,
                           match='decorated as parsers'):
            ge.Genius.loop(d, lambda x: x + 1)

        with pytest.raises(ValueError,
                           match='requires a header'):
            ge.Genius.loop(d, ge.parser(lambda x: x))

    def test_eval_condition(self):
        row = [1, 2, 3]
        assert ge.Genius.eval_condition(row, '0 > 0')
        assert not ge.Genius.eval_condition(row, '2 < 2')

        row = {'a': 1, 'b': 'foo'}
        assert ge.Genius.eval_condition(row, 'a == 1')
        assert ge.Genius.eval_condition(row, "b != 'bar'")


class TestPreprocess:
    def test_cleanse_gap(self):
        pp = ge.Preprocess()
        # First test doesn't use pp to verify staticmethod status.
        assert ge.Preprocess.cleanse_gap([1, 2, 3]) == [1, 2, 3]
        assert pp.cleanse_gap(['', '', '']) is None
        assert pp.cleanse_gap(['', '', ''], 0) == ['', '', '']
        assert pp.cleanse_gap([1, 2, None], 3) is None
        assert pp.cleanse_gap([1, 2, None], 2) == [1, 2, None]

    def test_detect_header(self):
        pp = ge.Preprocess()
        # First test doesn't use pp to verify staticmethod status.
        assert ge.Preprocess.detect_header([1, 2, 3]) is None
        assert pp.detect_header(['a', 'b', 'c']) == ['a', 'b', 'c']

    def test_extrapolate(self):
        assert ge.Preprocess.extrapolate(
            [2, None, None], [1, 2], [1, 'Foo', 'Bar']
        ) == [2, 'Foo', 'Bar']

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

        # Sanity check to ensure threshold works:
        d = Dataset(gaps)
        r = p.go(
            d, parser_args={'cleanse_gap': {'threshold': 0}},
            manual_header=['a', 'b', 'c', 'd'])
        assert r == gaps

    def test_custom_go(self):
        # Test custom preprocess step and header_func:
        pr = ge.parser(
            lambda x: [str(x[0]), *x[1:]],
            requires_header=False
        )
        hf = ge.parser(
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

    def test_go_w_extrapolate(self, needs_extrapolation):
        d = Dataset(needs_extrapolation)
        expected = [
            [1, 'StrexCorp', 'Teeth'],
            [2, 'StrexCorp', 'Radio Equipment'],
            [3, 'KVX Bank', 'Bribe'],
            [4, 'KVX Bank', 'Not candy or pens']
        ]

        assert ge.Preprocess().go(
            d,
            parser_args={'cleanse_gap': {'threshold': 1}},
            extrapolate=['vendor_name']
        ) == expected
