import os

import pandas as pd

from datagenius.io.odbc import ODBConnector


# These tests all share a single ODBConnector.
p = 'tests/samples/odbc_test.db'
o = ODBConnector()
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
        d = pd.DataFrame(sales[1], sales[0])
        o.insert('sales', d.to_dicts())
        assert o.select('sales') == d

    def test_drop_tbl(self):
        assert o.drop_tbl('sales')
        assert o.tables.get('sales') is None
        assert o.schemas.get('sales') is None


