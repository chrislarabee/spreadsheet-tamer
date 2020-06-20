import pandas as pd

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
        dict(id=3, name=3, price=3, cost=3, upc=3,
             attr1=2, attr2=2, attr3=0, attr4=0, attr5=0)
    ])
    df, md_dict = ex.count_uniques(pd.DataFrame(**products))
    pd.testing.assert_frame_equal(md_dict['metadata'], expected)


