import pandas as pd

from datagenius.io import text


def test_build_template(customers):
    t = text.build_template('tests/samples/csv/simple.csv')
    assert t == customers()['columns']

    t = text.build_template('tests/samples/excel/simple.xlsx')
    assert t == customers()['columns']
