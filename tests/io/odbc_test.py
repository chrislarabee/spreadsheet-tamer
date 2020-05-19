import os

from datagenius.io.odbc import ODBConnector
import datagenius.element as e

# These tests all share a single ODBConnector.
p = 'tests/samples/test.db'
o = ODBConnector()
o._db_path = p


class TestODBConnector:
    def test_purge_and_setup(self):
        o.purge()
        assert not os.path.exists(p)
        o.setup(p)
        assert os.path.exists(p)

    def test_new_table(self):
        sales_schema = dict(
            location=str,
            region=str,
            sales=int
        )
        o.new_tbl('sales', sales_schema)
        assert o._parse_sa_schema(o._tables['sales'].c) == sales_schema

    def test_insert_and_select(self, sales):
        d = e.Dataset(sales[1], sales[0])
        o.insert('sales', d.to_dicts())
        assert o.select('sales') == d


