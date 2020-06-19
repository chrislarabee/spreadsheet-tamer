import os

import pandas as pd

import datagenius.io.odbc as odbc


# These tests all share a single ODBConnector.
p = 'tests/samples/odbc_test.db'
o = odbc.ODBConnector()
o._db_path = p


class TestODBConnector:
    def test_purge_and_setup(self):
        o.purge()
        assert not os.path.exists(p)
        o.setup(p)
        assert os.path.exists(p)

    def test_new_tbl(self):
        sales_schema = dict(
            location=str,
            region=str,
            sales=int
        )
        o.new_tbl('sales', sales_schema)
        assert o._parse_sa_schema(o.tables['sales'].c) == sales_schema

    def test_insert_and_select(self, sales):
        d = pd.DataFrame(**sales)
        o.insert('sales', d)
        d2 = pd.DataFrame(o.select('sales'))
        pd.testing.assert_frame_equal(d2, d)

    def test_drop_tbl(self):
        assert o.drop_tbl('sales')
        assert o.tables.get('sales') is None
        assert o.schemas.get('sales') is None


def test_gen_schema():
    df = pd.DataFrame([
        dict(a=1, b='two', c=3.0),
        dict(a=4, b='five', c=6.0),
    ])
    expected = dict(a=int, b=str, c=float)
    assert odbc.gen_schema(df) == expected
