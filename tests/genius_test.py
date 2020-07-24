import pandas as pd
import pytest
from numpy import nan

import datagenius.genius as ge
import datagenius.util as u
import datagenius.lib.guides as gd


class TestGeniusAccessor:
    def test_preprocess(self, gaps, customers, gaps_totals):
        expected = pd.DataFrame(**customers())
        df = pd.DataFrame(gaps)
        df = u.purge_gap_rows(df)
        df, metadata = df.genius.preprocess()
        pd.testing.assert_frame_equal(df, expected)

        # More realistic test with a partial header:
        g = gaps_totals(False, False)
        expected = pd.DataFrame(g[1:], columns=g[0])
        df = pd.DataFrame.genius.from_file(
            'tests/samples/excel/gaps_totals.xlsx')
        df, metadata = df.genius.preprocess()
        pd.testing.assert_frame_equal(df, expected)

        # More realistic test with no header:
        expected = pd.DataFrame(**customers())
        df = pd.DataFrame.genius.from_file(
            'tests/samples/csv/gaps.csv')
        df, metadata = df.genius.preprocess()
        pd.testing.assert_frame_equal(df, expected)

    def test_explore(self, employees):
        df = pd.DataFrame(**employees)
        expected = pd.DataFrame([
            ['explore', 'count_values', 4, 4, 4, 2],
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
                [nan, nan, 800],
                [nan, nan, 1200],
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

    def test_standardize(self, needs_cleanse_typos, products):
        df = pd.DataFrame(**needs_cleanse_typos)
        expected = pd.DataFrame(**products)
        df, metadata = df.genius.standardize(
            cleaning_guides=dict(
                attr1=dict(cu='copper'),
                attr2=gd.CleaningGuide((('sm', 's'), 'small'))),
            type_mapping=dict(upc=str)
        )
        pd.testing.assert_series_equal(df['attr1'], expected['attr1'])
        assert df['upc'].dtype == 'O'

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

    def test_transmute(self, customers):
        expected = pd.DataFrame([
            [1, 'Yancy', 'Cordwainer', '00025']
        ], columns=['id', 'fname', 'lname', 'foreign_key'])
        df, md_dict = pd.DataFrame(**customers()).genius.transmute(
            ge.lib.clean.convert_types,
            ge.lib.clean.reject_on_conditions,
            type_mapping=dict(id=int),
            reject_conditions='id > 1'
        )
        pd.testing.assert_frame_equal(df, expected)

    def test_supplement(self, sales, regions, stores):
        df1 = pd.DataFrame(**sales)
        df2 = pd.DataFrame(**regions)
        result = df1.genius.supplement(
            df2,
            on=({'region': 'Northern'}, 'region'),
        )
        assert list(result.stores.fillna(0)) == [50.0, 50.0, 0, 0]
        assert list(result.employees.fillna(0)) == [500.0, 500.0, 0, 0]

        # Test split results:
        df1 = pd.DataFrame(**sales)
        df2 = pd.DataFrame(**regions)
        result = df1.genius.supplement(
            df2,
            on=({'region': 'Northern'}, 'region'),
            split_results=True
        )
        assert len(result) == 2
        assert list(result[0].stores) == [50.0, 50.0]
        assert list(result[0].employees) == [500.0, 500.0]
        assert set(result[1].columns).difference(
            {'location', 'region', 'sales'}) == set()

        # Test select columns functionality on exact match:
        df1 = pd.DataFrame(**sales)
        df2 = pd.DataFrame(**regions)
        result = df1.genius.supplement(
            df2,
            on='region',
            select_cols='stores'
        )
        assert list(result.stores) == [50, 50, 42, 42]
        assert list(result.region) == [
            'Northern', 'Northern', 'Southern', 'Southern']
        assert set(result.columns).difference({
            'region', 'stores', 'location', 'sales', 'merged_on'}
        ) == set()

        df1 = pd.DataFrame(**sales)
        df3 = pd.DataFrame(**stores)
        result = df1.genius.supplement(
            df3,
            on=gd.SupplementGuide('location', thresholds=.7, inexact=True),
            select_cols=('budget', 'location', 'other')
        )
        assert list(result.budget) == [100000, 90000, 110000, 90000]
        assert list(result.region) == [
            'Northern', 'Northern', 'Southern', 'Southern']
        assert set(result.columns).difference({
            'location', 'budget', 'region', 'sales',
            'location_A', 'merged_on'}) == set()
        
    def test_apply_strf(self, products):
        df = pd.DataFrame(**products)
        expected = pd.DataFrame([
            [1, 'WIDGET', 8.5, 4.0, 1234567890,
             nan, nan, nan, nan, nan],
            [2, 'DOOHICKEY', 9.99, 5.0, 2345678901,
             'Copper', 'Large', nan, nan, nan],
            [3, 'FLANGE', 1.0, 0.2, 3456789012,
             'Steel', 'Small', nan, nan, nan],
            [4, 'WHATSIT', 5.0, 2.0, 4567890123,
             'Aluminum', 'Small', nan, nan, nan]
        ], columns=[
            'id', 'name', 'price', 'cost', 'upc', 'attr1', 'attr2',
            'attr3', 'attr4', 'attr5']
        )
        df = df.genius.apply_strf('name', strf=str.upper)
        df = df.genius.apply_strf('attr1', 'attr2', strf=str.title)
        pd.testing.assert_frame_equal(df, expected)

        # Test col_strf_map:
        df = pd.DataFrame(**products)
        df = df.genius.apply_strf(
            attr1=str.title, attr2=str.title, name=str.upper)
        pd.testing.assert_frame_equal(df, expected)

        # Test mixed:
        df = pd.DataFrame(**products)
        df = df.genius.apply_strf(
            'attr1', 'attr2', 'upc', strf=str.title, name=str.upper)
        pd.testing.assert_frame_equal(df, expected)

    def test_multiapply(self):
        df = pd.DataFrame([
            dict(a=1, b=2, c=3),
            dict(a=4, b=5, c=6)
        ])
        expected = pd.DataFrame([
            dict(a=2, b=2, c=6),
            dict(a=8, b=5, c=12)
        ])
        df = df.genius.multiapply(lambda x: x * 2, 'a', 'c')
        pd.testing.assert_frame_equal(df, expected)

    def test_fillna_shift(self):
        df = pd.DataFrame([
            dict(a=1, b=nan, c=nan, d=2),
            dict(a=nan, b=3, c=4, d=nan),
            dict(a=nan, b=nan, c=5, d=nan)
        ])
        expected = pd.DataFrame([
            dict(a=1, b=2, c=nan, d=nan),
            dict(a=3, b=4, c=nan, d=nan),
            dict(a=5, b=nan, c=nan, d=nan)
        ])
        df2 = df.copy().genius.fillna_shift('a', 'b', 'c', 'd')
        pd.testing.assert_frame_equal(df2, expected, check_dtype=False)

        # Check rightward column order:
        expected = pd.DataFrame([
            dict(a=nan, b=nan, c=1, d=2),
            dict(a=nan, b=nan, c=3, d=4),
            dict(a=nan, b=nan, c=nan, d=5)
        ])
        df2 = df.copy().genius.fillna_shift('d', 'c', 'b', 'a')
        pd.testing.assert_frame_equal(df2, expected, check_dtype=False)

        # Check unusual column order:
        expected = pd.DataFrame([
            dict(a=nan, b=2, c=1, d=nan),
            dict(a=nan, b=3, c=4, d=nan),
            dict(a=nan, b=5, c=nan, d=nan)
        ])
        df2 = df.copy().genius.fillna_shift('b', 'c', 'd', 'a')
        pd.testing.assert_frame_equal(df2, expected, check_dtype=False)

        with pytest.raises(
                ValueError, match='Must supply at least 2 columns.'):
            df = df.genius.fillna_shift('a')

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
            dict(stage='h_preprocess', transmutation='purge_pre_header',
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

    def test_to_sqlite_dates(self):
        df = pd.DataFrame.genius.from_file(
            'tests/samples/excel/dt_nonsense.xlsx')
        df.genius.to_sqlite(
            'tests/samples', 'dt_nonsense', db_name='genius_test'
        )
        expected = pd.DataFrame([
            ['Eugene', '23', '2020-01-23 00:00:00', '2020-01-02 00:00:00',
             '2020-01-02 00:00:00']
        ], columns=[
            'collect_by', 'num_collected', 'date', 'ratio',
            'range']
        )
        df2 = pd.DataFrame.genius.from_file(
            'tests/samples', table='dt_nonsense', db_name='genius_test'
        )
        pd.testing.assert_frame_equal(expected, df2)

    def test_order_transmutations(self):
        x2 = u.transmutation(lambda x: x)
        x3 = u.transmutation(lambda x: x - 1)
        x1 = u.transmutation(lambda x: x + 1, priority=11)

        expected = [x1, x2, x3]

        assert pd.DataFrame.genius._order_transmutations(
            [x2, x3, x1]) == expected
