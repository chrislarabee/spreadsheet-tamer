from collections import OrderedDict as od

import pytest

from datagenius.element import Dataset, MetaData, TranslateRule, Mapping, MappingRule
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
    @ge.parser('breaks_loop')
    def g(x):
        return x + 1

    assert g.breaks_loop
    assert g.null_val is None

    @ge.parser(parses='set')
    def e(x):
        return x - 3
    assert e.parses == 'set'

    # Sanity check to ensure pre-built parsers work:
    assert not ge.Preprocess.cleanse_gaps.breaks_loop

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
        p, ps, rf = ge.ParserSubset.validate_steps(parsers)
        assert tuple(p) == parsers
        assert ps == 'row'
        assert rf == 'dicts'
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

        with pytest.raises(
                ValueError, match='same value for parses'):
            ge.ParserSubset.validate_steps((
                ge.parser(lambda w: w / 100, parses='set'),
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

    def test_apply_parsers(self):
        d = Dataset([
            [1, 2, 3],
            [4, 5, 6],
            [7, 8, 9]
        ])
        # Test simple binary filtering parser:
        p = ge.parser(lambda x: x if x[1] <= 2 else None,
                      'collect_rejects', requires_format='lists')
        assert ge.Genius.apply_parsers(
            d[0], p) == (False, True, True, [1, 2, 3])
        assert ge.Genius.apply_parsers(
            d[1], p) == (False, False, True, [4, 5, 6])

        # Test evaluative parser with args:
        p = ge.parser(lambda x, threshold: 1 if x[2] > threshold else 0,
                      requires_format='lists')
        assert ge.Genius.apply_parsers(
            d[0], p, threshold=5) == (False, True, False, 0)
        assert ge.Genius.apply_parsers(
            d[1], p, threshold=5) == (False, True, False, 1)
        # Ensure apply_parsers can handle parser_args:
        assert ge.Genius.apply_parsers(
            d[2], p, threshold=9, unused_kwarg=True) == (False, True, False, 0)

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
                      'breaks_loop', requires_format='lists')
        assert ge.Genius.loop_dataset(d, p) == [[2, 3, 4]]

        # Test args:
        p = ge.parser(lambda x, y: x if x[0] > y else None,
                      requires_format='lists')
        assert ge.Genius.loop_dataset(d, p, y=2) == [[3, 4, 5]]

        # Test condition:
        p = ge.parser(lambda x: x[0] + 1, requires_format='lists',
                      condition='0 <= 2')
        assert ge.Genius.loop_dataset(d, p) == [2, 3, [3, 4, 5]]

    def test_collect_rejects(self):
        d = Dataset([
            od(a=2, b=3, c=4)
        ])
        ge.Genius.collect_rejects(od(a=1, b=2, c=3), d)
        assert d.rejects == [[1, 2, 3]]

        ge.Genius.collect_rejects([7, 8, 9], d)
        assert d.rejects == [[1, 2, 3], [7, 8, 9]]

    def test_eval_condition(self):
        row = [1, 2, 3]
        assert ge.Genius.eval_condition(row, '0 > 0')
        assert not ge.Genius.eval_condition(row, '2 < 2')

        row = {'a': 1, 'b': 'foo'}
        assert ge.Genius.eval_condition(row, 'a == 1')
        assert ge.Genius.eval_condition(row, "b != 'bar'")


class TestPreprocess:
    def test_cleanse_gaps(self):
        pp = ge.Preprocess()
        # First test doesn't use pp to verify staticmethod status.
        assert ge.Preprocess.cleanse_gaps([1, 2, 3]) == [1, 2, 3]
        assert pp.cleanse_gaps(['', '', '']) is None
        assert pp.cleanse_gaps([1, 2, None]) == [1, 2, None]

    def test_detect_header(self):
        pp = ge.Preprocess()
        md = MetaData()
        # First test doesn't use pp to verify staticmethod status.
        assert ge.Preprocess.detect_header([1, 2, 3], md, 0) is None
        assert md.header_idx is None
        assert pp.detect_header(['a', 'b', 'c'], md, 7) == ['a', 'b', 'c']
        assert md.header == ['a', 'b', 'c']
        assert md.header_idx == 7
        # Test manual_header:
        assert pp.detect_header([1, 2, 3], md, None, ['x', 'y', 'z']) == [1, 2, 3]
        assert md.header == ['x', 'y', 'z']
        assert md.header_idx is None

    def test_nullify_empty_vals(self):
        expected = [None, 1, 'a', None]
        x = ge.Preprocess.nullify_empty_vals([None, 1, 'a', ''])
        assert x == expected
        assert isinstance(x, list)

        expected = od(a=None, b=None, c=1, d='foo')
        x = ge.Preprocess.nullify_empty_vals(od(a='', b='', c=1, d='foo'))
        assert x == expected
        assert isinstance(x, od)

        expected = dict(a=None, b=None, c=1)
        x = ge.Preprocess.nullify_empty_vals(dict(a='', b=None, c=1))
        assert x == expected
        assert isinstance(x, dict)

        # Test ignore functionality:
        expected = ['', None, 1, 'a']
        x = ge.Preprocess.nullify_empty_vals(['', '', 1, 'a'], ignore=(0,))
        assert x == expected

        expected = dict(a='', b=None, c=1)
        x = ge.Preprocess.nullify_empty_vals(dict(a='', b='', c=1), ignore=('a',))
        assert x == expected

    def test_cleanse_pre_header(self):
        x = [1, 2, 3]
        md = MetaData()
        md.header_idx = 4
        assert ge.Preprocess.cleanse_pre_header(x, md, 1) is None
        assert ge.Preprocess.cleanse_pre_header(x, md, 4) == x

    def test_normalize_whitespace(self):
        md = MetaData()
        assert ge.Preprocess.normalize_whitespace(
            ['a good string', ' a bad   string ', 1, None, 123.45], md
        ) == ['a good string', 'a bad string', 1, None, 123.45]
        assert md.white_space_cleaned == 1

    def test_basic_go(self, customers, simple_data, gaps, gaps_totals,
                      needs_cleanse_totals):
        p = ge.Preprocess()
        d = Dataset(simple_data())
        r = p.go(d)
        assert r == d
        assert r == customers[1]
        assert d.meta_data.header == customers[0]
        assert d.rejects == []

        d = Dataset(gaps)
        r = p.go(d, overwrite=False)
        assert r == customers[1]
        assert r != d
        assert r.meta_data != d.meta_data
        assert r.meta_data.header == customers[0]

        # Check full functionality:
        d = Dataset(gaps_totals())
        p.go(d)
        assert d == needs_cleanse_totals[1]
        assert d.meta_data.header == needs_cleanse_totals[0]
        assert d.rejects == [
            ['Sales by Location Report', None, None],
            ['Grouping: Region', None, None]
        ]

    def test_custom_go(self):
        # Test custom preprocess step and header_func:
        pr = ge.parser(
            lambda x: [str(x[0]), *x[1:]],
            requires_format='lists'
        )

        @ge.parser('breaks_loop', requires_format='lists', parses='set')
        def hf(x, meta_data):
            if x[0] == 'odd':
                meta_data.header = x
                return x
            else:
                return None

        d = Dataset([
            ['', '', ''],
            ['odd', 1, 'header'],
            [1, 2, 3],
            [None, None, None],
            [4, 5, 6]
        ])

        assert ge.Preprocess(pr, header_func=hf).go(d) == [
            ['1', 2, 3],
            ['4', 5, 6]
        ]
        assert d.meta_data.header == ['odd', 1, 'header']

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
        assert d.meta_data.header == ['a', 'b', 'c']


class TestClean:
    def test_extrapolate(self):
        assert ge.Clean.extrapolate(
            od(a=2, b=None, c=None),
            ['b', 'c'],
            od(a=1, b='Foo', c='Bar')
        ) == od(a=2, b='Foo', c='Bar')

    def test_apply_translations(self):
        expected = od(a=1, b=3, x=100)
        rules = (
            TranslateRule('a', {(1, ): 100}, 'x'),
            TranslateRule('b', {(2, ): 3})
        )
        assert ge.Clean.apply_translations(od(a=1, b=2), rules) == expected

    def test_cleanse_incomplete_rows(self):
        row = od(a=1, b=2, c=3, d=None, e=None)
        assert ge.Clean.cleanse_incomplete_rows(row, ['a', 'b']) == row
        assert ge.Clean.cleanse_incomplete_rows(row, ['a', 'd']) is None
        assert ge.Clean.cleanse_incomplete_rows(row, ['d']) is None

    def test_cast(self):
        d = od(a='1', b='2.0', c='test')
        rules = dict(a=float, b=int, c=str)
        assert ge.Clean.cast(d, rules) == od(a=1.0, b=2, c='test')
        d = od(a='1..23', b='1.23', c=None)
        assert ge.Clean.cast(d, rules) == od(a=1.23, b=1, c=None)

    def test_apply_capitalize(self):
        d = od(a='ALL CAPS', b='no caps', c='A mix OF Both')
        assert ge.Clean.apply_capitalize(d, ['a', 'b', 'c']) == od(
            a='All Caps', b='No Caps', c='A Mix Of Both'
        )

    def test_clean_numeric_typos(self):
        assert ge.Clean.clean_numeric_typos('1,9') == 1.9
        assert ge.Clean.clean_numeric_typos('10.1q') == 10.1
        assert ge.Clean.clean_numeric_typos('101q') == 101
        assert ge.Clean.clean_numeric_typos('1q0.1q') == 10.1
        assert ge.Clean.clean_numeric_typos('abc') == 'abc'

    def test_go_w_extrapolate(self, needs_extrapolation):
        d = Dataset(needs_extrapolation[1])
        d.meta_data.header = needs_extrapolation[0]
        expected = [
            od(
                product_id=1, vendor_name='StrexCorp', product_name='Teeth'),
            od(
                product_id=2, vendor_name='StrexCorp',
                product_name='Radio Equipment'),
            od(
                product_id=3, vendor_name='KVX Bank', product_name='Bribe'),
            od(
                product_id=4, vendor_name='KVX Bank',
                product_name='Not candy or pens')
        ]

        assert ge.Clean().go(
            d,
            extrapolate=['vendor_name']
        ) == expected

    def test_go_w_incomplete_rows(self, needs_cleanse_totals, sales):
        d = Dataset(needs_cleanse_totals[1], needs_cleanse_totals[0]).to_dicts()
        expected = Dataset(sales[1], sales[0]).to_dicts()

        assert ge.Clean().go(d, required_columns=['location']) == expected._data

    def test_go_w_translations(self, needs_translation, products):
        d = Dataset(needs_translation[1], needs_translation[0])
        p = Dataset(products[1], products[0]).to_dicts()

        assert ge.Clean().go(
            d,
            translation_rules=(
                TranslateRule('attr1', {'cu': 'copper'}),
                TranslateRule('attr2', {'sm': 'small'})
            )
        ) == p._data


class TestExplore:
    def test_types_report(self):
        md = MetaData()
        ge.Explore.types_report([1, 2, 3, '4'], 'prob_numeric', md)
        assert md['prob_numeric'] == {
            'string_pct': 0, 'numeric_pct': 1, 'probable_type': 'numeric'
        }

        ge.Explore.types_report([1, 2, 'x'], 'less_prob_num', md)
        assert md['less_prob_num'] == {
            'string_pct': 0.33, 'numeric_pct': 0.67, 'probable_type': 'numeric'
        }

        ge.Explore.types_report([1, 'x', 'y'], 'prob_str', md)
        assert md['prob_str'] == {
            'string_pct': 0.67, 'numeric_pct': 0.33, 'probable_type': 'string'
        }

        ge.Explore.types_report([], 'uncertain', md)
        assert md['uncertain'] == {
            'string_pct': 0, 'numeric_pct': 0, 'probable_type': 'uncertain'
        }

    def test_uniques_report(self):
        md = MetaData()
        ge.Explore.uniques_report([1, 2, 3, 4], 'id', md)
        assert md['id'] == dict(
            unique_ct=4,
            primary_key=True
        )

        ge.Explore.uniques_report(['x', 'x', 'y', 'y'], 'vars', md)
        assert md['vars'] == dict(
            unique_ct=2,
            primary_key=False
        )

    def test_go(self):
        d = Dataset([
            [1, 2, 'a'],
            [4, 5, 'b'],
            [None, 7, 'c']
        ])

        ge.Explore().go(d)
        assert d.data_orientation == 'column'

        assert d.meta_data == {
            '0': {
                'unique_ct': 3, 'primary_key': True, 'string_pct': 0.33,
                'numeric_pct': 0.67, 'probable_type': 'numeric', 'null_ct': 1,
                'nullable': True
            },
            '1': {
                'unique_ct': 3, 'primary_key': True, 'string_pct': 0.0,
                'numeric_pct': 1.0, 'probable_type': 'numeric', 'null_ct': 0,
                'nullable': False
            },
            '2': {
                'unique_ct': 3, 'primary_key': True, 'string_pct': 1.0,
                'numeric_pct': 0.0, 'probable_type': 'string', 'null_ct': 0,
                'nullable': False
            }
        }


class TestReformat:
    def test_do_mapping(self):
        m = Mapping(['a', 'b', 'c'], rules=dict(
            a=MappingRule('x'),
            b='y',
            c=MappingRule('z', 3)
        ))
        assert ge.Reformat(m).do_mapping(od(x=1, y=2, z=None)) == od(
            a=1, b=2, c=3
        )
