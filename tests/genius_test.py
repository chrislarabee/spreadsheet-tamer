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
        df = df.genius.preprocess()
        pd.testing.assert_frame_equal(df, expected)

        g = gaps_totals(False, False)
        expected = pd.DataFrame(g[1:], columns=g[0])
        df = pd.DataFrame(gaps_totals())
        df = u.purge_gap_rows(df)
        df = df.genius.preprocess()
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
        
    def test_to_sqlite(self, sales):
        d = pd.DataFrame(**sales)
        o = odbc.ODBConnector()
        d.genius.to_sqlite(
            'tests/samples', 'sales', db_conn=o, db_name='element_test')
        d2 = pd.DataFrame.genius.from_file(
            'tests/samples/', table='sales', db_conn=o, db_name='element_test')
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
# class TestClean:
#     def test_extrapolate(self):
#         assert ge.Clean.extrapolate(
#             od(a=2, b=None, c=None),
#             ['b', 'c'],
#             od(a=1, b='Foo', c='Bar')
#         ) == od(a=2, b='Foo', c='Bar')
#
#     def test_apply_rules(self):
#         expected = od(a=1, b=3, x=100)
#         rules = (
#             e.Rule({(1, ): 100}, 'a', to='x'),
#             e.Rule({(2, ): 3}, 'b')
#         )
#         assert ge.Clean.apply_rules(od(a=1, b=2), rules) == expected
#
#     def test_cleanse_incomplete_rows(self):
#         row = od(a=1, b=2, c=3, d=None, e=None)
#         assert ge.Clean.cleanse_incomplete_rows(row, ['a', 'b']) == row
#         assert ge.Clean.cleanse_incomplete_rows(row, ['a', 'd']) is None
#         assert ge.Clean.cleanse_incomplete_rows(row, ['d']) is None
#
#     def test_clean_numeric_typos(self):
#         assert ge.Clean.clean_numeric_typos('1,9') == 1.9
#         assert ge.Clean.clean_numeric_typos('10.1q') == 10.1
#         assert ge.Clean.clean_numeric_typos('101q') == 101
#         assert ge.Clean.clean_numeric_typos('1q0.1q') == 10.1
#         assert ge.Clean.clean_numeric_typos('abc') == 'abc'
#
#     def test_go_w_extrapolate(self, needs_extrapolation):
#         d = e.Dataset(needs_extrapolation[1])
#         d.meta_data.header = needs_extrapolation[0]
#         expected = [
#             od(
#                 product_id=1, vendor_name='StrexCorp', product_name='Teeth'),
#             od(
#                 product_id=2, vendor_name='StrexCorp',
#                 product_name='Radio Equipment'),
#             od(
#                 product_id=3, vendor_name='KVX Bank', product_name='Bribe'),
#             od(
#                 product_id=4, vendor_name='KVX Bank',
#                 product_name='Not candy or pens')
#         ]
#
#         assert ge.Clean().go(
#             d,
#             extrapolate=['vendor_name']
#         ) == expected
#
#     def test_go_w_incomplete_rows(self, needs_cleanse_totals, sales):
#         d = e.Dataset(needs_cleanse_totals[1], needs_cleanse_totals[0]).to_dicts()
#         expected = e.Dataset(sales[1], sales[0]).to_dicts()
#
#         assert ge.Clean().go(d, required_columns=['location']) == expected._data
#
#     def test_go_w_rules(self, needs_rules, products):
#         d = e.Dataset(needs_rules[1], needs_rules[0])
#         p = e.Dataset(products[1], products[0]).to_dicts()
#
#         assert ge.Clean().go(
#             d,
#             data_rules=(
#                 e.Rule({'cu': 'copper'}, 'attr1'),
#                 e.Rule({'sm': 'small'}, 'attr2')
#             )
#         ) == p._data
#
#
# class TestExplore:
#     def test_types_report(self):
#         md = e.MetaData()
#         ge.Explore.types_report([1, 2, 3, '4'], 'prob_numeric', md)
#         assert md['prob_numeric'] == {
#             'string_pct': 0, 'numeric_pct': 1, 'probable_type': 'numeric'
#         }
#
#         ge.Explore.types_report([1, 2, 'x'], 'less_prob_num', md)
#         assert md['less_prob_num'] == {
#             'string_pct': 0.33, 'numeric_pct': 0.67, 'probable_type': 'numeric'
#         }
#
#         ge.Explore.types_report([1, 'x', 'y'], 'prob_str', md)
#         assert md['prob_str'] == {
#             'string_pct': 0.67, 'numeric_pct': 0.33, 'probable_type': 'string'
#         }
#
#         ge.Explore.types_report([], 'uncertain', md)
#         assert md['uncertain'] == {
#             'string_pct': 0, 'numeric_pct': 0, 'probable_type': 'uncertain'
#         }
#
#     def test_uniques_report(self):
#         md = e.MetaData()
#         ge.Explore.uniques_report([1, 2, 3, 4], 'id', md)
#         assert md['id'] == dict(
#             unique_ct=4,
#             primary_key=True
#         )
#
#         ge.Explore.uniques_report(['x', 'x', 'y', 'y'], 'vars', md)
#         assert md['vars'] == dict(
#             unique_ct=2,
#             primary_key=False
#         )
#
#     def test_go(self):
#         d = e.Dataset([
#             [1, 2, 'a'],
#             [4, 5, 'b'],
#             [None, 7, 'c']
#         ])
#
#         ge.Explore().go(d)
#         assert d.data_orientation == 'column'
#
#         assert d.meta_data == {
#             '0': {
#                 'unique_ct': 3, 'primary_key': True, 'string_pct': 0.33,
#                 'numeric_pct': 0.67, 'probable_type': 'numeric', 'null_ct': 1,
#                 'nullable': True
#             },
#             '1': {
#                 'unique_ct': 3, 'primary_key': True, 'string_pct': 0.0,
#                 'numeric_pct': 1.0, 'probable_type': 'numeric', 'null_ct': 0,
#                 'nullable': False
#             },
#             '2': {
#                 'unique_ct': 3, 'primary_key': True, 'string_pct': 1.0,
#                 'numeric_pct': 0.0, 'probable_type': 'string', 'null_ct': 0,
#                 'nullable': False
#             }
#         }
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
        df1 = pd.DataFrame(sales[1], columns=sales[0])
        df2 = pd.DataFrame(regions[1], columns=regions[0])
        s = ge.Supplement(({'region': 'Northern'}, 'region'))
        result = s(df1, df2)
        assert list(result.stores.fillna(0)) == [50.0, 50.0, 0, 0]
        assert list(result.employees.fillna(0)) == [500.0, 500.0, 0, 0]

        # Test split results:
        df1 = pd.DataFrame(sales[1], columns=sales[0])
        df2 = pd.DataFrame(regions[1], columns=regions[0])
        s = ge.Supplement(({'region': 'Northern'}, 'region'))
        result = s(df1, df2, split_results=True)
        assert len(result) == 2
        assert list(result[0].stores) == [50.0, 50.0]
        assert list(result[0].employees) == [500.0, 500.0]
        assert set(result[1].columns).difference(
            {'location', 'region', 'sales'}) == set()

        # Test select columns functionality on exact match:
        df1 = pd.DataFrame(sales[1], columns=sales[0])
        df2 = pd.DataFrame(regions[1], columns=regions[0])
        s = ge.Supplement('region', select_cols='stores')
        result = s(df1, df2)
        assert list(result.stores) == [50, 50, 42, 42]
        assert list(result.region) == [
            'Northern', 'Northern', 'Southern', 'Southern']
        assert set(result.columns).difference({
            'region', 'stores', 'location', 'sales', 'merged_on'}
        ) == set()

        df1 = pd.DataFrame(sales[1], columns=sales[0])
        df3 = pd.DataFrame(stores[1], columns=stores[0])
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
        df1 = pd.DataFrame(sales[1], columns=sales[0])
        df2 = pd.DataFrame(regions[1], columns=regions[0])
        result = ge.Supplement.do_exact(df1, df2, ('region',))
        assert list(result.stores) == [50, 50, 42, 42]
        assert list(result.employees) == [500, 500, 450, 450]

    def test_do_inexact(self, sales, regions, stores):
        # Make sure inexact can replicate exact, just as a sanity
        # check:
        df1 = pd.DataFrame(sales[1], columns=sales[0])
        df2 = pd.DataFrame(regions[1], columns=regions[0])
        result = ge.Supplement.do_inexact(
            df1, df2, ('region',), thresholds=(1,))
        assert list(result.stores) == [50, 50, 42, 42]
        assert list(result.employees) == [500, 500, 450, 450]

        # Now for a real inexact match:
        df3 = pd.DataFrame(stores[1], columns=stores[0])
        result = ge.Supplement.do_inexact(
            df1, df3, ('location',), thresholds=(.7,))
        assert list(result.budget) == [100000, 90000, 110000, 90000]
        assert list(result.inventory) == [5000, 4500, 4500, 4500]
        assert set(result.columns).difference({
            'location', 'region', 'region_s', 'sales', 'location_s',
            'budget', 'inventory'}) == set()

        # Same match, but with block:
        df3 = pd.DataFrame(stores[1], columns=stores[0])
        result = ge.Supplement.do_inexact(
            df1, df3, ('location',), thresholds=(.7,), block=('region',))
        assert list(result.budget) == [100000, 90000, 110000, 90000]
        assert list(result.inventory) == [5000, 4500, 4500, 4500]
        assert set(result.columns).difference({
            'location', 'region', 'region_s', 'sales', 'location_s',
            'budget', 'inventory'}) == set()

        # Same match, but with multiple ons:
        df3 = pd.DataFrame(stores[1], columns=stores[0])
        result = ge.Supplement.do_inexact(
            df1, df3, ('location', 'region'), thresholds=(.7, 1))
        assert list(result.budget) == [100000, 90000, 110000, 90000]
        assert list(result.inventory) == [5000, 4500, 4500, 4500]
        assert set(result.columns).difference({
            'location', 'region', 'region_s', 'sales', 'location_s',
            'budget', 'inventory'}) == set()

    def test_chunk_dframes(self, stores, sales, regions):
        df = pd.DataFrame(stores[1], columns=stores[0])
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
        df1 = pd.DataFrame(sales[1], columns=sales[0])
        df2 = pd.DataFrame(regions[1], columns=regions[0])
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
        df1 = pd.DataFrame(sales[1], columns=sales[0])
        df2 = pd.DataFrame(regions[1], columns=regions[0])
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
        df = pd.DataFrame(stores[1], columns=stores[0])
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
