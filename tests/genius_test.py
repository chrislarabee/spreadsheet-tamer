import pandas as pd
import numpy as np

import datagenius.element as e
import datagenius.genius as ge
import datagenius.util as u


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

    def test_explore(self, employees):
        df = pd.DataFrame(**employees)
        expected = pd.DataFrame([
            ['explore', 'count_uniques', 4, 2, 4, 1],
            ['explore', 'count_nulls', 0, 0, 0, 2],
            ['explore', 'collect_data_types',
             'int(1.0)', 'str(1.0)', 'str(1.0)', 'float(0.5),nan(0.5)']
        ], columns=[
            'stage', 'transmutation', 'employee_id', 'department',
            'name', 'wfh_stipend'
        ])
        df, metadata = df.genius.explore()
        pd.testing.assert_frame_equal(metadata.collected, expected)

    def test_clean(self, sales, needs_cleanse_totals):
        df = pd.DataFrame(**needs_cleanse_totals)
        expected = pd.DataFrame(**sales).iloc[1:3].reset_index(drop=True)
        df, metadata = df.genius.clean(
            required_cols=['location'],
            reject_str_content=dict(location='Bayside'),
            reject_conditions='sales < 300'
        )
        pd.testing.assert_frame_equal(df, expected)

        expected_metadata = pd.DataFrame([
            ['clean', 'reject_incomplete_rows', 0, 0, 2.0],
            ['clean', 'reject_on_conditions', 1.0, 1, 1.0],
            ['clean', 'reject_on_str_content', 1.0, 1.0, 1.0],
        ], columns=[
            'stage', 'transmutation', 'location', 'region', 'sales'
        ])
        pd.testing.assert_frame_equal(metadata.collected, expected_metadata)

        expected_rejects = pd.DataFrame(
            columns=['location', 'region', 'sales'],
            data=[
                [np.nan, np.nan, 800],
                [np.nan, np.nan, 1200],
                ['Kalliope Store', 'Southern', 200],
                ['Bayside Store', 'Northern', 500],
            ]
        )
        pd.testing.assert_frame_equal(metadata.rejects, expected_rejects)

    def test_reformat(self, products, formatted_products):
        df = pd.DataFrame(**products)
        expected = pd.DataFrame(**formatted_products)
        expected['material'] = expected['material'].fillna('plastic')
        df, metadata = df.genius.reformat(
            reformat_template=[
                'Prod Id', 'Name', 'Price', 'Cost', 'Prod UPC',
                'Material', 'Barcode'],
            reformat_mapping=dict(
                id='Prod Id', name='Name', price='Price', cost='Cost',
                upc=('Prod UPC', 'Barcode'), attr1='Material'),
            defaults_mapping=dict(material='plastic')
        )
        pd.testing.assert_frame_equal(df, expected)

    def test_align_tms_with_options(self):
        tms = [
            ge.lib.clean.reject_on_conditions,
            ge.lib.clean.reject_on_str_content,
            ge.lib.clean.reject_incomplete_rows
        ]
        expected = [
            ge.lib.clean.reject_on_conditions
        ]
        assert pd.DataFrame.genius._align_tms_with_options(
            tms, dict(reject_conditions='a == 1')
        ) == expected

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

#     def test_order_parsers(self):
#         x2 = ge.parser(lambda x: x)
#         x3 = ge.parser(lambda x: x - 1)
#         x1 = ge.parser(lambda x: x + 1, priority=11)
#
#         expected = [x1, x2, x3]
#
#         assert ge.Genius.order_parsers([x2, x3, x1]) == expected


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
