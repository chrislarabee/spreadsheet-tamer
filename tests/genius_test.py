from collections import OrderedDict as od

import pytest
import pandas as pd
import numpy as np

import datagenius.element as e
import datagenius.genius as ge
import datagenius.util as u


# def test_parser():
#     # Decorator without arguments:
#     @ge.parser
#     def f(x):
#         return x * 10
#
#     assert f.is_parser
#     assert not f.breaks_loop
#     assert f.null_val is None
#
#     # Decorator with arguments:
#     @ge.parser('breaks_loop')
#     def g(x):
#         return x + 1
#
#     assert g.breaks_loop
#     assert g.null_val is None
#
#     @ge.parser(parses='set')
#     def e(x):
#         return x - 3
#     assert e.parses == 'set'
#
#     # Sanity check to ensure pre-built parsers work:
#     assert not ge.Preprocess.cleanse_gaps.breaks_loop
#
#     # Sanity check to ensure lambda function parsers work:
#     p = ge.parser(lambda x: x + 1, null_val=0)
#
#     assert p.null_val == 0
#     assert p(3) == 4
#
#
# class TestParserSubset:
#     def test_general(self):
#         parsers = (
#             ge.parser(lambda x: x + 1),
#             ge.parser(lambda y: y * 2)
#         )
#
#         subset = ge.ParserSubset(*parsers)
#         assert tuple(subset) == parsers
#
#         assert [*subset] == list(parsers)
#
#     def test_validate_steps(self):
#         parsers = (
#             ge.parser(lambda x: x + 1),
#             ge.parser(lambda y: y * 2)
#         )
#         p, ps, rf = ge.ParserSubset.validate_steps(parsers)
#         assert tuple(p) == parsers
#         assert ps == 'row'
#         assert rf == 'dicts'
#         with pytest.raises(
#             ValueError, match='only take parser functions'
#         ):
#             ge.ParserSubset.validate_steps(('string', parsers))
#
#         with pytest.raises(
#                 ValueError, match='same value for requires_format'):
#             ge.ParserSubset.validate_steps((
#                 ge.parser(lambda z: z ** 2, requires_format='lists'),
#                 *parsers
#             ))
#
#         with pytest.raises(
#                 ValueError, match='same value for parses'):
#             ge.ParserSubset.validate_steps((
#                 ge.parser(lambda w: w / 100, parses='set'),
#                 *parsers
#             ))
from datagenius.io import odbc


class TestGeniusAccessor:
    def test_preprocess(self, gaps, customers, gaps_totals):
        expected = pd.DataFrame(**customers())
        df = pd.DataFrame(gaps)
        df = u.purge_gap_rows(df)
        df, metadata = df.genius.preprocess()
        pd.testing.assert_frame_equal(df, expected)

        g = gaps_totals(False, False)
        expected = pd.DataFrame(g[1:], columns=g[0])
        df = pd.DataFrame(gaps_totals())
        df = u.purge_gap_rows(df)
        df, metadata = df.genius.preprocess()
        pd.testing.assert_frame_equal(df, expected)

    def test_from_file(self, customers):
        df = pd.DataFrame.genius.from_file(
            'tests/samples/csv/simple.csv')
        pd.testing.assert_frame_equal(
            df, pd.DataFrame(**customers(), dtype='object')
        )

        # Ensure null rows are being dropped from csv:
        df = pd.DataFrame.genius.from_file(
            'tests/samples/csv/gaps.csv')
        assert df.shape == (5, 4)

        df = pd.DataFrame.genius.from_file(
            'tests/samples/excel/simple.xlsx')
        pd.testing.assert_frame_equal(
            df, pd.DataFrame(**customers(int), dtype='object')
        )

        # Ensure null rows are being dropped from excel:
        df = pd.DataFrame.genius.from_file(
            'tests/samples/excel/gaps_totals.xlsx')
        assert df.shape == (8, 3)

        # Test pulling from sqlite db:
        df = pd.DataFrame.genius.from_file(
            'tests/samples/sqlite', table='customers', db_name='read_testing')
        pd.testing.assert_frame_equal(
            df, pd.DataFrame(**customers())
        )
        assert isinstance(df, pd.DataFrame)

    def test_to_sqlite_metadata(self, gaps_totals):
        df = pd.DataFrame(gaps_totals())
        df, metadata = df.genius.preprocess()
        df.genius.to_sqlite('tests/samples', 'sales',
                            db_name='genius_test', metadata=metadata)
        md_df = pd.DataFrame.genius.from_file(
            'tests/samples', table='sales_metadata', db_name='genius_test'
        )
        expected = pd.DataFrame([
            dict(stage='preprocess', transmutation='purge_pre_header',
                 location=2.0, region=0.0, sales=0.0),
            dict(stage='preprocess', transmutation='normalize_whitespace',
                 location=0.0, region=0.0, sales=0.0),
        ])
        pd.testing.assert_frame_equal(md_df, expected)
        
    def test_to_sqlite(self, products):
        d = pd.DataFrame(**products)
        d.genius.to_sqlite(
            'tests/samples', 'products', db_name='genius_test')
        d2 = pd.DataFrame.genius.from_file(
            'tests/samples/', table='products', db_name='genius_test')
        pd.testing.assert_frame_equal(d, d2)


# class TestGenius:
#     def test_validate_steps(self):
#         parsers = (
#             ge.parser(lambda x: x + 1),
#             ge.parser(lambda y: y * 2)
#         )
#         subset = ge.ParserSubset(*parsers)
#         assert tuple(ge.Genius.validate_steps(
#             (*parsers, subset))) == (*parsers, subset)
#         with pytest.raises(
#             ValueError,
#             match='only take parser functions or ParserSubset'
#         ):
#             ge.Genius.validate_steps(('string', parsers))
#
#         with pytest.raises(
#             ValueError, match='ParserSubset object'
#         ):
#             ge.Genius.validate_steps((
#                 ge.parser(lambda z: z * 10),
#                 parsers
#             ))
#
#     def test_order_parsers(self):
#         x2 = ge.parser(lambda x: x)
#         x3 = ge.parser(lambda x: x - 1)
#         x1 = ge.parser(lambda x: x + 1, priority=11)
#
#         expected = [x1, x2, x3]
#
#         assert ge.Genius.order_parsers([x2, x3, x1]) == expected
#
#     def test_apply_parsers(self):
#         d = e.Dataset([
#             [1, 2, 3],
#             [4, 5, 6],
#             [7, 8, 9]
#         ])
#         # Test simple binary filtering parser:
#         p = ge.parser(lambda x: x if x[1] <= 2 else None,
#                       'collect_rejects', requires_format='lists')
#         assert ge.Genius.apply_parsers(
#             d[0], p) == (False, True, True, [1, 2, 3])
#         assert ge.Genius.apply_parsers(
#             d[1], p) == (False, False, True, [4, 5, 6])
#
#         # Test evaluative parser with args:
#         p = ge.parser(lambda x, threshold: 1 if x[2] > threshold else 0,
#                       requires_format='lists')
#         assert ge.Genius.apply_parsers(
#             d[0], p, threshold=5) == (False, True, False, 0)
#         assert ge.Genius.apply_parsers(
#             d[1], p, threshold=5) == (False, True, False, 1)
#         # Ensure apply_parsers can handle parser_args:
#         assert ge.Genius.apply_parsers(
#             d[2], p, threshold=9, unused_kwarg=True) == (False, True, False, 0)
#
#     def test_loop_dataset(self, simple_data):
#         # Test simple filtering loop_dataset:
#         expected = [
#             ['1', 'Yancy', 'Cordwainer', '00025'],
#             ['2', 'Muhammad', 'El-Kanan', '00076'],
#             ['3', 'Luisa', 'Romero', '00123'],
#         ]
#         d = e.Dataset(simple_data())
#         p = ge.parser(lambda x: (x if len(x[2]) > 5 else None),
#                       requires_format='lists')
#         assert ge.Genius.loop_dataset(d, p) == expected
#
#         # Test loop_dataset that generates new values:
#         p = ge.parser(lambda x: 1 if len(x[2]) > 5 else 0,
#                       requires_format='lists')
#         expected = [0, 1, 1, 1, 0]
#         assert ge.Genius.loop_dataset(d, p) == expected
#
#         # Test breaks_loop
#         d = e.Dataset([
#             [1, 2, 3],
#             [2, 3, 4],
#             [3, 4, 5]
#         ])
#
#         p = ge.parser(lambda x: x if x[0] > 1 else None,
#                       'breaks_loop', requires_format='lists')
#         assert ge.Genius.loop_dataset(d, p) == [[2, 3, 4]]
#
#         # Test args:
#         p = ge.parser(lambda x, y: x if x[0] > y else None,
#                       requires_format='lists')
#         assert ge.Genius.loop_dataset(d, p, y=2) == [[3, 4, 5]]
#
#         # Test condition:
#         p = ge.parser(lambda x: x[0] + 1, requires_format='lists',
#                       condition='0 <= 2')
#         assert ge.Genius.loop_dataset(d, p) == [2, 3, [3, 4, 5]]
#
#     def test_collect_rejects(self):
#         d = e.Dataset([
#             od(a=2, b=3, c=4)
#         ])
#         ge.Genius.collect_rejects(od(a=1, b=2, c=3), d)
#         assert d.rejects == [[1, 2, 3]]
#
#         ge.Genius.collect_rejects([7, 8, 9], d)
#         assert d.rejects == [[1, 2, 3], [7, 8, 9]]

    # def test_eval_condition(self):
    #     row = [1, 2, 3]
    #     assert ge.Genius.eval_condition(row, '0 > 0')
    #     assert not ge.Genius.eval_condition(row, '2 < 2')
    #
    #     row = {'a': 1, 'b': 'foo'}
    #     assert ge.Genius.eval_condition(row, 'a == 1')
    #     assert ge.Genius.eval_condition(row, "b != 'bar'")
    #
    #     row = {'a': 'list, of, strings', 'b': 'foo'}
    #     assert ge.Genius.eval_condition(row, '"list" in a')
#
#     def test_basic_go(self, customers, simple_data, gaps, gaps_totals,
#                       needs_cleanse_totals):
#         p = ge.Preprocess()
#         d = e.Dataset(simple_data())
#         r = p.go(d)
#         assert r == d
#         assert r == customers[1]
#         assert d.meta_data.header == customers[0]
#         assert d.rejects == []
#
#         d = e.Dataset(gaps)
#         r = p.go(d, overwrite=False)
#         assert r == customers[1]
#         assert r != d
#         assert r.meta_data != d.meta_data
#         assert r.meta_data.header == customers[0]
#
#         # Check full functionality:
#         d = e.Dataset(gaps_totals())
#         p.go(d)
#         assert d == needs_cleanse_totals[1]
#         assert d.meta_data.header == needs_cleanse_totals[0]
#         assert d.rejects == [
#             ['Sales by Location Report', None, None],
#             ['Grouping: Region', None, None]
#         ]
#
#     def test_custom_go(self):
#         # Test custom preprocess step and header_func:
#         pr = ge.parser(
#             lambda x: [str(x[0]), *x[1:]],
#             requires_format='lists'
#         )
#
#         @ge.parser('breaks_loop', requires_format='lists', parses='set')
#         def hf(x, meta_data):
#             if x[0] == 'odd':
#                 meta_data.header = x
#                 return x
#             else:
#                 return None
#
#         d = e.Dataset([
#             ['', '', ''],
#             ['odd', 1, 'header'],
#             [1, 2, 3],
#             [None, None, None],
#             [4, 5, 6]
#         ])
#
#         assert ge.Preprocess(pr, header_func=hf).go(d) == [
#             ['1', 2, 3],
#             ['4', 5, 6]
#         ]
#         assert d.meta_data.header == ['odd', 1, 'header']
#
#         # Test manual_header:
#         d = e.Dataset([
#             [1, 2, 3],
#             [4, 5, 6]
#         ])
#
#         assert ge.Preprocess().go(
#             d,
#             manual_header=['a', 'b', 'c']) == [
#             [1, 2, 3],
#             [4, 5, 6]
#         ]
#         assert d.meta_data.header == ['a', 'b', 'c']
#
#
# class TestReformat:
#     def test_go(self, products, formatted_products):
#         m = e.Mapping(
#             formatted_products[0],
#             e.Rule({None: 'plastic'}, 'attr1', to='Material'),
#             e.Rule({None: None}, 'upc', to=('Prod UPC', 'Barcode')),
#             id='ProdId',
#             name='Name',
#             price='Price',
#             cost='Cost',
#             attr2='Size'
#         )
#
#         d = e.Dataset(products[1], products[0])
#         d2 = e.Dataset(formatted_products[1], formatted_products[0]).to_dicts()
#         assert ge.Reformat(m).go(d)._data == d2._data


class TestSupplement:
    def test_call(self, sales, regions, stores):
        df1 = pd.DataFrame(**sales)
        df2 = pd.DataFrame(**regions)
        s = ge.Supplement(({'region': 'Northern'}, 'region'))
        result = s(df1, df2)
        assert list(result.stores.fillna(0)) == [50.0, 50.0, 0, 0]
        assert list(result.employees.fillna(0)) == [500.0, 500.0, 0, 0]

        # Test split results:
        df1 = pd.DataFrame(**sales)
        df2 = pd.DataFrame(**regions)
        s = ge.Supplement(({'region': 'Northern'}, 'region'))
        result = s(df1, df2, split_results=True)
        assert len(result) == 2
        assert list(result[0].stores) == [50.0, 50.0]
        assert list(result[0].employees) == [500.0, 500.0]
        assert set(result[1].columns).difference(
            {'location', 'region', 'sales'}) == set()

        # Test select columns functionality on exact match:
        df1 = pd.DataFrame(**sales)
        df2 = pd.DataFrame(**regions)
        s = ge.Supplement('region', select_cols='stores')
        result = s(df1, df2)
        assert list(result.stores) == [50, 50, 42, 42]
        assert list(result.region) == [
            'Northern', 'Northern', 'Southern', 'Southern']
        assert set(result.columns).difference({
            'region', 'stores', 'location', 'sales', 'merged_on'}
        ) == set()

        df1 = pd.DataFrame(**sales)
        df3 = pd.DataFrame(**stores)
        s = ge.Supplement(
            e.MatchRule('location', thresholds=.7, inexact=True),
            select_cols=('budget', 'location', 'other'))
        result = s(df1, df3)
        assert list(result.budget) == [100000, 90000, 110000, 90000]
        assert list(result.region) == [
            'Northern', 'Northern', 'Southern', 'Southern']
        assert set(result.columns).difference({
            'location', 'budget', 'region', 'sales',
            'location_A', 'merged_on'}) == set()

    def test_do_exact(self, sales, regions):
        df1 = pd.DataFrame(**sales)
        df2 = pd.DataFrame(**regions)
        result = ge.Supplement.do_exact(df1, df2, ('region',))
        assert list(result.stores) == [50, 50, 42, 42]
        assert list(result.employees) == [500, 500, 450, 450]

    def test_do_inexact(self, sales, regions, stores):
        # Make sure inexact can replicate exact, just as a sanity
        # check:
        df1 = pd.DataFrame(**sales)
        df2 = pd.DataFrame(**regions)
        result = ge.Supplement.do_inexact(
            df1, df2, ('region',), thresholds=(1,))
        assert list(result.stores) == [50, 50, 42, 42]
        assert list(result.employees) == [500, 500, 450, 450]

        # Now for a real inexact match:
        df3 = pd.DataFrame(**stores)
        result = ge.Supplement.do_inexact(
            df1, df3, ('location',), thresholds=(.7,))
        assert list(result.budget) == [100000, 90000, 110000, 90000]
        assert list(result.inventory) == [5000, 4500, 4500, 4500]
        assert set(result.columns).difference({
            'location', 'region', 'region_s', 'sales', 'location_s',
            'budget', 'inventory'}) == set()

        # Same match, but with block:
        df3 = pd.DataFrame(**stores)
        result = ge.Supplement.do_inexact(
            df1, df3, ('location',), thresholds=(.7,), block=('region',))
        assert list(result.budget) == [100000, 90000, 110000, 90000]
        assert list(result.inventory) == [5000, 4500, 4500, 4500]
        assert set(result.columns).difference({
            'location', 'region', 'region_s', 'sales', 'location_s',
            'budget', 'inventory'}) == set()

        # Same match, but with multiple ons:
        df3 = pd.DataFrame(**stores)
        result = ge.Supplement.do_inexact(
            df1, df3, ('location', 'region'), thresholds=(.7, 1))
        assert list(result.budget) == [100000, 90000, 110000, 90000]
        assert list(result.inventory) == [5000, 4500, 4500, 4500]
        assert set(result.columns).difference({
            'location', 'region', 'region_s', 'sales', 'location_s',
            'budget', 'inventory'}) == set()

    def test_chunk_dframes(self, stores, sales, regions):
        df = pd.DataFrame(**stores)
        plan = ge.Supplement.build_plan((
            ({'budget': (90000,)}, 'location'),
            ({'inventory': (4500,)}, 'budget')
        ))
        c, p_df = ge.Supplement.chunk_dframes(plan, df)
        assert c[0][0].to_dict('records') == [
            dict(location='W Valley', region='Northern', budget=90000,
                 inventory=4500),
            dict(location='Kalliope', region='Southern', budget=90000,
                 inventory=4500)
        ]
        assert c[1][0].to_dict('records') == [
            dict(location='Precioso', region='Southern', budget=110000,
                 inventory=4500)
        ]
        assert p_df.to_dict('records') == [
            dict(location='Bayside', region='Northern', budget=100000,
                 inventory=5000)
        ]
        # Test multiple dframes:
        df1 = pd.DataFrame(**sales)
        df2 = pd.DataFrame(**regions)
        # Test with no conditions:
        plan = ge.Supplement.build_plan((
            ({None: (None,)}, 'region'),
        ))
        c, p_df = ge.Supplement.chunk_dframes(plan, df1, df2)
        assert c[0][0].to_dict('records') == [
            dict(location='Bayside Store', region='Northern', sales=500),
            dict(location='West Valley Store', region='Northern', sales=300),
            dict(location='Precioso Store', region='Southern', sales=1000),
            dict(location='Kalliope Store', region='Southern', sales=200),
        ]
        assert c[0][1].to_dict('records') == [
            dict(region='Northern', stores=50, employees=500),
            dict(region='Southern', stores=42, employees=450)
        ]
        assert p_df.to_dict('records') == []
        # Test with conditions
        df1 = pd.DataFrame(**sales)
        df2 = pd.DataFrame(**regions)
        plan = ge.Supplement.build_plan((
            ({'region': ('Northern',)}, 'region'),
        ))
        c, p_df = ge.Supplement.chunk_dframes(plan, df1, df2)
        assert c[0][0].to_dict('records') == [
            dict(location='Bayside Store', region='Northern', sales=500),
            dict(location='West Valley Store', region='Northern', sales=300),
        ]
        assert c[0][1].to_dict('records') == [
            dict(region='Northern', stores=50, employees=500)
        ]
        assert p_df.to_dict('records') == [
            dict(location='Precioso Store', region='Southern', sales=1000),
            dict(location='Kalliope Store', region='Southern', sales=200),
        ]

    def test_slice_dframe(self, stores):
        df = pd.DataFrame(**stores)
        expected = [
            dict(location='W Valley', region='Northern', budget=90000,
                 inventory=4500),
            dict(location='Precioso', region='Southern', budget=110000,
                 inventory=4500),
            dict(location='Kalliope', region='Southern', budget=90000,
                 inventory=4500)
        ]
        x = ge.Supplement.slice_dframe(df, {'inventory': (4500,)})
        assert df is not x[0]
        assert x[0].to_dict('records') == expected
        assert x[1]

        # Test multiple conditions:
        expected = [
            dict(location='W Valley', region='Northern', budget=90000,
                 inventory=4500),
            dict(location='Kalliope', region='Southern', budget=90000,
                 inventory=4500)
        ]
        x = ge.Supplement.slice_dframe(df, {'inventory': (4500,),
                                            'budget': (90000,)})
        assert x[0].to_dict('records') == expected
        assert x[1]

        # Test no conditions:
        expected = [
            dict(location='Bayside', region='Northern', budget=100000,
                 inventory=5000),
            dict(location='W Valley', region='Northern', budget=90000,
                 inventory=4500),
            dict(location='Precioso', region='Southern', budget=110000,
                 inventory=4500),
            dict(location='Kalliope', region='Southern', budget=90000,
                 inventory=4500)
        ]
        x = ge.Supplement.slice_dframe(df, {None: (None,)})
        assert x[0].to_dict('records') == expected
        assert x[1]

        # Test unmet conditions:
        expected = []
        x = ge.Supplement.slice_dframe(df, {'budget': (25000,)})
        assert x[0].to_dict('records') == expected
        assert not x[1]

    def test_build_plan(self):
        plan = ge.Supplement.build_plan(('a', 'b', 'c'))
        assert plan[0].output() == (('a', 'b', 'c'), {None: (None,)})

        # Check that condition/on pairs can be in any order:
        plan = ge.Supplement.build_plan(('a', 'b', ('a', {'c': 'x'})))

        assert plan[0].output() == (('a',), {'c': ('x',)})
        assert plan[1].output() == (('a', 'b'), {None: (None,)})

        plan = ge.Supplement.build_plan(('a', 'b', ({'c': 'x'}, 'a')))
        assert plan[0].output() == (('a',), {'c': ('x',)})
        assert plan[1].output() == (('a', 'b'), {None: (None,)})

        plan = ge.Supplement.build_plan((({'c': 'x'}, 'a'),))
        assert plan[0].output() == (('a',), {'c': ('x',)})
        assert len(plan) == 1
