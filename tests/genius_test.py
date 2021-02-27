import pandas as pd
import pytest
from numpy import nan
from datetime import datetime as dt

import datagenius.genius as ge
import datagenius.util as u
import datagenius.lib.guides as gd
from tests import testing_tools


class TestGeniusAccessor:
    def test_reformat(self, products, formatted_products):
        df = pd.DataFrame(**products)
        expected = pd.DataFrame(**formatted_products)
        expected["material"] = expected["material"].fillna("plastic")
        df, metadata = df.genius.reformat(
            reformat_template=[
                "Prod Id",
                "Name",
                "Price",
                "Cost",
                "Prod UPC",
                "Material",
                "Barcode",
            ],
            reformat_mapping=dict(
                id="Prod Id",
                name="Name",
                price="Price",
                cost="Cost",
                upc=("Prod UPC", "Barcode"),
                attr1="Material",
            ),
            defaults_mapping=dict(material="plastic"),
        )
        pd.testing.assert_frame_equal(df, expected)

    def test_align_tms_with_options(self):
        tms = [
            ge.lib.clean.reject_on_conditions,
            ge.lib.clean.reject_on_str_content,
            ge.lib.clean.reject_incomplete_rows,
        ]
        expected = [ge.lib.clean.reject_on_conditions]
        assert (
            pd.DataFrame.genius._align_tms_with_options(
                tms, dict(reject_conditions="a == 1")
            )
            == expected
        )

    def test_transmute(self, customers):
        expected = pd.DataFrame(
            [[1, "Yancy", "Cordwainer", "00025"]],
            columns=["id", "fname", "lname", "foreign_key"],
        )
        df, md_dict = pd.DataFrame(**customers()).genius.transmute(
            ge.lib.clean.convert_types,
            ge.lib.clean.reject_on_conditions,
            type_mapping=dict(id=int),
            reject_conditions="id > 1",
        )
        pd.testing.assert_frame_equal(df, expected)

    def test_supplement(self, sales, regions, stores):
        df1 = pd.DataFrame(**sales)
        df2 = pd.DataFrame(**regions)
        result = df1.genius.supplement(
            df2,
            on=({"region": "Northern"}, "region"),
        )
        assert list(result.stores.fillna(0)) == [50.0, 50.0, 0, 0]
        assert list(result.employees.fillna(0)) == [500.0, 500.0, 0, 0]

        # Test split results:
        df1 = pd.DataFrame(**sales)
        df2 = pd.DataFrame(**regions)
        result = df1.genius.supplement(
            df2, on=({"region": "Northern"}, "region"), split_results=True
        )
        assert len(result) == 2
        assert list(result[0].stores) == [50.0, 50.0]
        assert list(result[0].employees) == [500.0, 500.0]
        assert (
            set(result[1].columns).difference({"location", "region", "sales"}) == set()
        )

        # Test select columns functionality on exact match:
        df1 = pd.DataFrame(**sales)
        df2 = pd.DataFrame(**regions)
        result = df1.genius.supplement(df2, on="region", select_cols="stores")
        assert list(result.stores) == [50, 50, 42, 42]
        assert list(result.region) == ["Northern", "Northern", "Southern", "Southern"]
        assert (
            set(result.columns).difference(
                {"region", "stores", "location", "sales", "merged_on"}
            )
            == set()
        )

        df1 = pd.DataFrame(**sales)
        df3 = pd.DataFrame(**stores)
        result = df1.genius.supplement(
            df3,
            on=gd.SupplementGuide("location", thresholds=0.7, inexact=True),
            select_cols=("budget", "location", "other"),
        )
        assert list(result.budget) == [100000, 90000, 110000, 90000]
        assert list(result.region) == ["Northern", "Northern", "Southern", "Southern"]
        assert (
            set(result.columns).difference(
                {"location", "budget", "region", "sales", "location_A", "merged_on"}
            )
            == set()
        )

    def test_to_sqlite_metadata(self, gaps_totals):
        df = pd.DataFrame(gaps_totals())
        df, metadata = df.genius.preprocess()
        df.genius.to_sqlite(
            "tests/samples", "sales", db_name="genius_test", metadata=metadata
        )
        md_df = pd.DataFrame.genius.from_file(
            "tests/samples", table="sales_metadata", db_name="genius_test"
        )
        expected = pd.DataFrame(
            [
                dict(
                    stage="h_preprocess",
                    transmutation="purge_pre_header",
                    location=2.0,
                    region=0.0,
                    sales=0.0,
                ),
                dict(
                    stage="preprocess",
                    transmutation="normalize_whitespace",
                    location=0.0,
                    region=0.0,
                    sales=0.0,
                ),
            ]
        )
        pd.testing.assert_frame_equal(md_df, expected)

    def test_to_sqlite(self, products):
        d = pd.DataFrame(data=products["data"][:3], columns=products["columns"])
        d.genius.to_sqlite("tests/samples", "products", db_name="genius_test")
        d2 = pd.DataFrame.genius.from_file(
            "tests/samples/", table="products", db_name="genius_test"
        )
        pd.testing.assert_frame_equal(d, d2)

        expected = pd.DataFrame(**products)
        d3 = pd.DataFrame(data=[products["data"][-1]], columns=products["columns"])
        d3.genius.to_sqlite(
            "tests/samples", "products", db_name="genius_test", drop_first=False
        )
        d4 = pd.DataFrame.genius.from_file(
            "tests/samples/", table="products", db_name="genius_test"
        )
        pd.testing.assert_frame_equal(d4, expected)

    def test_to_sqlite_dates(self):
        df = pd.DataFrame.genius.from_file("tests/samples/excel/dt_nonsense.xlsx")
        df.genius.to_sqlite("tests/samples", "dt_nonsense", db_name="genius_test")
        expected = pd.DataFrame(
            [
                [
                    "Eugene",
                    "23",
                    "2020-01-23 00:00:00",
                    "2020-01-02 00:00:00",
                    "2020-01-02 00:00:00",
                ]
            ],
            columns=["collect_by", "num_collected", "date", "ratio", "range"],
        )
        df2 = pd.DataFrame.genius.from_file(
            "tests/samples", table="dt_nonsense", db_name="genius_test"
        )
        pd.testing.assert_frame_equal(expected, df2)

    @pytest.mark.sheets_api
    def test_to_from_gsheet(self, sheets_api):
        testing_tools.check_sheets_api_skip(sheets_api)
        df = pd.DataFrame([dict(a=1, b=2), dict(a=3, b=4)])
        name = f"data_genius_genius_test_sheet {dt.now()}"
        sheet_id, shape = df.genius.to_gsheet(name, s_api=sheets_api)
        testing_tools.created_ids.append(sheet_id)
        expected = pd.DataFrame(
            [["a", "b"], ["1", "2"], ["3", "4"]], columns=["0", "1"]
        )
        assert shape == (3, 2)
        read_df = pd.DataFrame.genius.from_file(name + ".sheet", s_api=sheets_api)
        pd.testing.assert_frame_equal(read_df, expected)

    def test_from_file(self, customers):
        df = pd.DataFrame.genius.from_file("tests/samples/csv/customers.csv")
        pd.testing.assert_frame_equal(df, pd.DataFrame(**customers(), dtype="object"))

        # Ensure null rows are being dropped from csv:
        df = pd.DataFrame.genius.from_file("tests/samples/csv/gaps.csv")
        assert df.shape == (5, 4)

        df = pd.DataFrame.genius.from_file("tests/samples/excel/customers.xlsx")
        pd.testing.assert_frame_equal(
            df, pd.DataFrame(**customers(int), dtype="object")
        )

        # Ensure null rows are being dropped from excel:
        df = pd.DataFrame.genius.from_file("tests/samples/excel/sales_report.xlsx")
        assert df.shape == (8, 3)

        # Test pulling from sqlite db:
        df = pd.DataFrame.genius.from_file(
            "tests/samples/sqlite", table="customers", db_name="read_testing"
        )
        pd.testing.assert_frame_equal(df, pd.DataFrame(**customers()))
        assert isinstance(df, pd.DataFrame)

    def test_order_transmutations(self):
        x2 = u.transmutation(lambda x: x)
        x3 = u.transmutation(lambda x: x - 1)
        x1 = u.transmutation(lambda x: x + 1, priority=11)

        expected = [x1, x2, x3]

        assert pd.DataFrame.genius._order_transmutations([x2, x3, x1]) == expected
