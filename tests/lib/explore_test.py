import pandas as pd
from numpy import nan

import datagenius.lib.explore as ex


def test_count_uniques(customers, sales, products):
    expected = pd.DataFrame([
        dict(id=4, fname=4, lname=4, foreign_key=4)
    ])
    df, md_dict = ex.count_uniques(pd.DataFrame(**customers()))
    pd.testing.assert_frame_equal(md_dict['metadata'], expected)

    expected = pd.DataFrame([
        dict(location=4, region=2, sales=4)
    ])
    df, md_dict = ex.count_uniques(pd.DataFrame(**sales))
    pd.testing.assert_frame_equal(md_dict['metadata'], expected)

    expected = pd.DataFrame([
        dict(id=4, name=4, price=4, cost=4, upc=4,
             attr1=3, attr2=2, attr3=0, attr4=0, attr5=0)
    ])
    df, md_dict = ex.count_uniques(pd.DataFrame(**products))
    pd.testing.assert_frame_equal(md_dict['metadata'], expected)


def test_count_nulls(products):
    expected = pd.DataFrame([
        dict(id=0, name=0, price=0, cost=0, upc=0,
             attr1=1, attr2=1, attr3=4, attr4=4, attr5=4)
    ])
    df, md_dict = ex.count_nulls(pd.DataFrame(**products))
    pd.testing.assert_frame_equal(md_dict['metadata'], expected)


def test_collect_data_types():
    df = pd.DataFrame([
        dict(a=1, b=2.4, c='string'),
        dict(a=2, b='x', c=nan)
    ])
    expected = pd.DataFrame([
        dict(a='int(1.0)', b='float(0.5),str(0.5)', c='nan(0.5),str(0.5)')
    ])
    df, md_dict = ex.collect_data_types(df)
    pd.testing.assert_frame_equal(md_dict['metadata'], expected)


def test_check_type_violations():
    df = pd.DataFrame([
        dict(a=1, b=2.4, c='string'),
        dict(a=2, b='x', c=nan)
    ])
    expected = pd.DataFrame([
        dict(a=False, b=True, c=False)
    ])
    df, md_dict = ex.check_type_violations(
        df, dict(a=int, b=float, c=str))
    pd.testing.assert_frame_equal(md_dict['metadata'], expected)
