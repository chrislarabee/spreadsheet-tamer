import pandas as pd
from numpy import nan

import datagenius.lib.clean as cl
import datagenius.element as e


def test_complete_clusters(needs_extrapolation, employees):
    df = pd.DataFrame(**needs_extrapolation)
    df, md_dict = cl.complete_clusters(df, ['department'])
    pd.testing.assert_frame_equal(df, pd.DataFrame(**employees))
    expected_metadata = pd.DataFrame([
        dict(department=2)
    ])
    pd.testing.assert_frame_equal(md_dict['metadata'], expected_metadata)

    df = pd.DataFrame([
        dict(a=1, b=2, c=3),
        dict(a=nan, b=nan, c=nan),
        dict(a=1, b=nan, c=4),
        dict(a=nan, b=nan, c=nan),
    ])
    expected = pd.DataFrame([
        dict(a=1.0, b=2.0, c=3.0),
        dict(a=1.0, b=2.0, c=3.0),
        dict(a=1.0, b=2.0, c=4.0),
        dict(a=1.0, b=2.0, c=4.0),
    ])
    df, md_dict = cl.complete_clusters(df, ['a', 'b', 'c'])
    pd.testing.assert_frame_equal(df, expected)
    expected_metadata = pd.DataFrame([
        dict(a=2, b=3, c=2)
    ])
    pd.testing.assert_frame_equal(md_dict['metadata'], expected_metadata)


def test_reject_incomplete_rows(needs_cleanse_totals, sales):
    df = pd.DataFrame(**needs_cleanse_totals)
    df, md_dict = cl.reject_incomplete_rows(df, ['location', 'region'])
    pd.testing.assert_frame_equal(df, pd.DataFrame(**sales))
    expected_metadata = pd.DataFrame([
        dict(location=0, region=0, sales=2)
    ])
    pd.testing.assert_frame_equal(md_dict['metadata'], expected_metadata)
    expected_rejects = pd.DataFrame([
        dict(location=nan, region=nan, sales=800),
        dict(location=nan, region=nan, sales=1200),
    ], index=[2, 5])
    pd.testing.assert_frame_equal(md_dict['rejects'], expected_rejects,
                                  check_dtype=False)


def test_reject_on_conditions(employees):
    df = pd.DataFrame(**employees)
    df2, md_dict = cl.reject_on_conditions(df, "department == 'Sales'")
    pd.testing.assert_frame_equal(df[2:].reset_index(drop=True), df2)
    expected_metadata = pd.DataFrame([
        dict(employee_id=2, department=2, name=2, wfh_stipend=1)
    ])
    pd.testing.assert_frame_equal(md_dict['metadata'], expected_metadata)

    df2, md_dict = cl.reject_on_conditions(
        df, ("department == 'Customer Service'", "employee_id == 4"))
    pd.testing.assert_frame_equal(df.iloc[:3].reset_index(drop=True), df2)
    expected_metadata = pd.DataFrame([
        dict(employee_id=1, department=1, name=1, wfh_stipend=0)
    ])
    pd.testing.assert_frame_equal(md_dict['metadata'], expected_metadata)


def test_reject_on_str_content(customers):
    df = pd.DataFrame(**customers())
    df2, md_dict = cl.reject_on_str_content(df, dict(foreign_key='25'))
    pd.testing.assert_frame_equal(df[1:].reset_index(drop=True), df2)
    expected_metadata = pd.DataFrame([
        dict(id=1, fname=1, lname=1, foreign_key=1)
    ])
    pd.testing.assert_frame_equal(md_dict['metadata'], expected_metadata)


def test_cleanse_typos(needs_cleanse_typos):
    df = pd.DataFrame(**needs_cleanse_typos)
    df2, md_dict = cl.cleanse_typos(
        df,
        dict(
            attr1=dict(cu='copper'),
            attr2=e.CleaningGuide((('sm', 's'), 'small')))
    )
    pd.testing.assert_frame_equal(df, df2)
    expected_metadata = pd.DataFrame([
        dict(id=0, name=0, price=0, cost=0, upc=0, attr1=1, attr2=2,
             attr3=0, attr4=0, attr5=0)
    ])
    pd.testing.assert_frame_equal(md_dict['metadata'], expected_metadata)


def test_convert_types(customers):
    df = pd.DataFrame(**customers())
    df2, md_dict = cl.convert_types(
        df, {'id': int, 'foreign_key': e.ZeroNumeric})
    pd.testing.assert_frame_equal(df2, pd.DataFrame(**customers(int)))
    assert list(df2.dtypes) == ['int64', 'O', 'O', 'O']
    expected_metadata = pd.DataFrame([
        dict(id=4, fname=0, lname=0, foreign_key=4)
    ])
    pd.testing.assert_frame_equal(md_dict['metadata'], expected_metadata)
