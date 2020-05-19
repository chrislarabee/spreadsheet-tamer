from collections import OrderedDict

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


class TestParserSubset:
    def test_general(self):
        parsers = (
            ge.parser(lambda x: x + 1),
            ge.parser(lambda y: y * 2)
        )

        subset = ge.ParserSubset(*parsers)
        assert tuple(subset) == parsers

        assert [*subset] == list(parsers)

    def test_validate_steps(self):
        parsers = (
            ge.parser(lambda x: x + 1),
            ge.parser(lambda y: y * 2)
        )
        assert tuple(ge.ParserSubset.validate_steps(parsers)) == parsers
        with pytest.raises(
            ValueError, match='only take parser functions'
        ):
            ge.ParserSubset.validate_steps(('string', parsers))

        with pytest.raises(
                ValueError, match='same value for requires_format'):
            ge.ParserSubset.validate_steps((
                ge.parser(lambda z: z ** 2, requires_format='lists'),
                *parsers
            ))


class TestGenius:
    def test_validate_steps(self):
        parsers = (
            ge.parser(lambda x: x + 1),
            ge.parser(lambda y: y * 2)
        )
        subset = ge.ParserSubset(*parsers)
        assert tuple(ge.Genius.validate_steps(
            (*parsers, subset))) == (*parsers, subset)
        with pytest.raises(
            ValueError,
            match='only take parser functions or ParserSubset'
        ):
            ge.Genius.validate_steps(('string', parsers))

        with pytest.raises(
            ValueError, match='ParserSubset object'
        ):
            ge.Genius.validate_steps((
                ge.parser(lambda z: z * 10),
                parsers
            ))

    def test_order_parsers(self):
        x2 = ge.parser(lambda x: x)
        x3 = ge.parser(lambda x: x - 1)
        x1 = ge.parser(lambda x: x + 1, priority=11)

        expected = [x1, x2, x3]

        assert ge.Genius.order_parsers([x2, x3, x1]) == expected

    def test_loop_dataset(self, simple_data):
        # Test simple filtering loop_dataset:
        expected = [
            ['1', 'Yancy', 'Cordwainer', '00025'],
            ['2', 'Muhammad', 'El-Kanan', '00076'],
            ['3', 'Luisa', 'Romero', '00123'],
        ]
        d = Dataset(simple_data())
        p = ge.parser(lambda x: (x if len(x[2]) > 5 else None),
                      requires_format='lists')
        assert ge.Genius.loop_dataset(d, p) == expected

        # Test loop_dataset that generates new values:
        p = ge.parser(lambda x: 1 if len(x[2]) > 5 else 0,
                      requires_format='lists')
        expected = [0, 1, 1, 1, 0]
        assert ge.Genius.loop_dataset(d, p) == expected

        # Test breaks_loop
        d = Dataset([
            [1, 2, 3],
            [2, 3, 4],
            [3, 4, 5]
        ])

        p = ge.parser(lambda x: x if x[0] > 1 else None,
                      requires_format='lists', breaks_loop=True)
        assert ge.Genius.loop_dataset(d, p) == [[2, 3, 4]]

        # Test args:
        @ge.parser(requires_format='lists', takes_args=True)
        def arg_parser(x, y):
            return x if x[0] > y else None
        assert ge.Genius.loop_dataset(
            d, arg_parser, parser_args={
                'arg_parser': {'y': 2}}
        ) == [[3, 4, 5]]

        # Test condition:
        @ge.parser(requires_format='lists', condition='0 <= 2')
        def conditional_parser(x):
            x.append(0)
            return x
        assert ge.Genius.loop_dataset(
            d, conditional_parser
        ) == [
            [1, 2, 3, 0],
            [2, 3, 4, 0],
            [3, 4, 5]
        ]

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
            requires_format='lists'
        )
        hf = ge.parser(
            lambda x: x if x[0] == 'odd' else None,
            requires_format='lists',
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


class TestClean:
    def test_extrapolate(self):
        assert ge.Clean.extrapolate(
            OrderedDict(a=2, b=None, c= None),
            ['b', 'c'],
            OrderedDict(a=1, b='Foo', c='Bar')
        ) == OrderedDict(a=2, b='Foo', c='Bar')

    def test_clean_numeric_typos(self):
        assert ge.Clean.clean_numeric_typos('1,9') == 1.9
        assert ge.Clean.clean_numeric_typos('10.1q') == 10.1
        assert ge.Clean.clean_numeric_typos('101q') == 101
        assert ge.Clean.clean_numeric_typos('1q0.1q') == 10.1
        assert ge.Clean.clean_numeric_typos('abc') == 'abc'

    def test_go_w_extrapolate(self, needs_extrapolation):
        d = Dataset(needs_extrapolation[1])
        d.header = needs_extrapolation[0]
        expected = [
            OrderedDict(
                product_id=1, vendor_name='StrexCorp', product_name='Teeth'),
            OrderedDict(
                product_id=2, vendor_name='StrexCorp',
                product_name='Radio Equipment'),
            OrderedDict(
                product_id=3, vendor_name='KVX Bank', product_name='Bribe'),
            OrderedDict(
                product_id=4, vendor_name='KVX Bank',
                product_name='Not candy or pens')
        ]

        assert ge.Clean().go(
            d,
            extrapolate=['vendor_name']
        ) == expected


class TestExplore:
    def test_types_report(self):
        assert ge.Explore.types_report([1, 2, 3, '4']) == {
            'str_pct': 0, 'num_pct': 1, 'probable_type': 'numeric'
        }

        assert ge.Explore.types_report([1, 2, 'x']) == {
            'str_pct': 0.33, 'num_pct': 0.67, 'probable_type': 'numeric'
        }

        assert ge.Explore.types_report([1, 'x', 'y']) == {
            'str_pct': 0.67, 'num_pct': 0.33, 'probable_type': 'string'
        }

        assert ge.Explore.types_report([]) == {
            'str_pct': 0, 'num_pct': 0, 'probable_type': 'uncertain'
        }

    def test_uniques_report(self):
        assert ge.Explore.uniques_report([1, 2, 3, 4]) == {
            'unique_ct': 4, 'unique_values': 'primary_key'
        }

        assert ge.Explore.uniques_report(['x', 'x', 'y', 'y']) == {
            'unique_ct': 2, 'unique_values': {'x', 'y'}
        }
