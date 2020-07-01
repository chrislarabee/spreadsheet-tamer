import pandas as pd
from numpy import nan

import datagenius.lib.explore as ex


def test_count_values(employees):
    # Data type compatibility check:
    df = pd.DataFrame([
        dict(a=[1, 2, 3], b=2),
        dict(a=[4, 5, 6], b=3)
    ])
    expected = pd.DataFrame([dict(a=2, b=2)])
    df, md_dict = ex.count_values(df)
    pd.testing.assert_frame_equal(md_dict['metadata'], expected)

    expected = pd.DataFrame([
        dict(employee_id=4, department=4, name=4, wfh_stipend=2)
    ])
    df, md_dict = ex.count_values(pd.DataFrame(**employees))
    pd.testing.assert_frame_equal(md_dict['metadata'], expected)


def test_count_uniques(customers, sales, products):
    # Data type compatibility check:
    df = pd.DataFrame([
        dict(a=[1, 2, 3], b=2),
        dict(a=[4, 5, 6], b=3)
    ])
    expected = pd.DataFrame([dict(a=2, b=2)])
    df, md_dict = ex.count_uniques(df)
    pd.testing.assert_frame_equal(md_dict['metadata'], expected)

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
    # Data type compatibility check:
    df = pd.DataFrame([
        dict(a=[1, 2, 3], b=2),
        dict(a=[4, 5, 6], b=3)
    ])
    expected = pd.DataFrame([dict(a=0, b=0)])
    df, md_dict = ex.count_nulls(df)
    pd.testing.assert_frame_equal(md_dict['metadata'], expected)

    expected = pd.DataFrame([
        dict(id=0, name=0, price=0, cost=0, upc=0,
             attr1=1, attr2=1, attr3=4, attr4=4, attr5=4)
    ])
    df, md_dict = ex.count_nulls(pd.DataFrame(**products))
    pd.testing.assert_frame_equal(md_dict['metadata'], expected)


def test_collect_data_types():
    df = pd.DataFrame([
        dict(a=1, b=2.4, c='string'),
        dict(a=2, b='x', c=nan),
        dict(a=3, b=2, c='string2')
    ])
    expected = pd.DataFrame([
        dict(a='int(1.0)',
             b='float(0.33),int(0.33),str(0.33)',
             c='nan(0.33),str(0.67)')
    ])
    df, md_dict = ex.collect_data_types(df)
    pd.testing.assert_frame_equal(md_dict['metadata'], expected)


def test_id_type_violations():
    df = pd.DataFrame([
        dict(a=1, b=2.4, c='string'),
        dict(a=2, b='x', c=nan)
    ])
    expected = pd.DataFrame([
        dict(a=False, b=True, c=False)
    ])
    df, md_dict = ex.id_type_violations(
        df, dict(a=int, b=float, c=str))
    pd.testing.assert_frame_equal(md_dict['metadata'], expected)


def test_id_nullable_violations():
    df = pd.DataFrame([
        dict(a=1, b=nan, c='x'),
        dict(a=nan, b=2, c='y')
    ])
    expected = pd.DataFrame([
        dict(a=False, b=True, c=False)
    ])
    df, md_dict = ex.id_nullable_violations(df, ('b', 'c'))
    pd.testing.assert_frame_equal(md_dict['metadata'], expected)


def test_id_clustering_violations():
    df = pd.DataFrame([
        dict(a='w', b='i', c=1),
        dict(a='x', b='j', c=2),
        dict(a='x', b='j', c=2),
        dict(a='x', b='j', c=nan),
        dict(a='x', b='k', c=1),
        dict(a='x', b='k', c=2),
    ])
    expected_cols = [
        'cluster_id', 'row_ct', 'c_ct', 'rn', 'c_invalid', 'invalid']
    expected = pd.DataFrame([
        [0, 1, 1, 1, False, False],
        [1, 3, 1, 1, True, True],
        [1, 3, 1, 2, True, True],
        [1, 3, 1, 3, True, True],
        [2, 2, 2, 1, False, False],
        [2, 2, 2, 2, False, False]
    ], columns=expected_cols)
    df, md_dict = ex.id_clustering_violations(
        df, ['a', 'b'], ['c']
    )
    pd.testing.assert_frame_equal(df[expected_cols], expected)
    expected_metadata = pd.DataFrame([
        dict(a=0, b=0, c=3)
    ])
    pd.testing.assert_frame_equal(md_dict['metadata'], expected_metadata)
