import pandas as pd
from numpy import nan
import pytest

import datagenius.lib.clean as cl
import datagenius.lib.guides as gd
import datagenius.element as e


class TestCleaningGuide:
    def test_basics(self):
        cg = gd.CleaningGuide(
            ('a', 'x'),
            (('b', 'c'), 'y'),
            d='z'
        )
        assert cg('a') == 'x'
        assert cg('b') == 'y'
        assert cg('c') == 'y'
        assert cg('d') == 'z'
        assert cg('e') == 'e'

    def test_convert(self):
        cg = gd.CleaningGuide.convert(
            gd.CleaningGuide(
                ('a', 'x'),
                (('b', 'c'), 'y'),
                d='z'
            )
        )
        assert cg('a') == 'x'
        assert cg('b') == 'y'
        assert cg('c') == 'y'
        assert cg('d') == 'z'
        assert cg('e') == 'e'

        cg = gd.CleaningGuide.convert(
            dict(a='x', b='y', c='z')
        )
        assert cg('a') == 'x'
        assert cg('b') == 'y'
        assert cg('c') == 'z'
        assert cg('e') == 'e'

        with pytest.raises(
                ValueError,
                match="Invalid object=test, type=<class 'str'>"):
            cg = gd.CleaningGuide.convert('test')


def test_complete_clusters(needs_extrapolation, employees):
    df = pd.DataFrame(**needs_extrapolation)
    df, md_dict = cl.complete_clusters(df, ['department'])
    pd.testing.assert_frame_equal(df, pd.DataFrame(**employees))
    expected_metadata = pd.DataFrame([
        dict(department=2)
    ])
    pd.testing.assert_frame_equal(
        md_dict['metadata'], expected_metadata)

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
    pd.testing.assert_frame_equal(
        md_dict['metadata'], expected_metadata)


def test_reject_incomplete_rows(needs_cleanse_totals, sales):
    df = pd.DataFrame(**needs_cleanse_totals)
    df, md_dict = cl.reject_incomplete_rows(
        df, ['location', 'region'])
    pd.testing.assert_frame_equal(df, pd.DataFrame(**sales))
    expected_metadata = pd.DataFrame([
        dict(location=0, region=0, sales=2)
    ])
    pd.testing.assert_frame_equal(
        md_dict['metadata'], expected_metadata)
    expected_rejects = pd.DataFrame([
        dict(location=nan, region=nan, sales=800),
        dict(location=nan, region=nan, sales=1200),
    ], index=[2, 5])
    pd.testing.assert_frame_equal(
        md_dict['rejects'], expected_rejects, check_dtype=False)


def test_reject_on_conditions(employees):
    df = pd.DataFrame(**employees)
    df2, md_dict = cl.reject_on_conditions(
        df, "department == 'Sales'")
    pd.testing.assert_frame_equal(df[2:].reset_index(drop=True), df2)
    expected_metadata = pd.DataFrame([
        dict(employee_id=2, department=2, name=2, wfh_stipend=1)
    ])
    pd.testing.assert_frame_equal(
        md_dict['metadata'], expected_metadata)

    df2, md_dict = cl.reject_on_conditions(
        df, ("department == 'Customer Service'", "employee_id == 4"))
    pd.testing.assert_frame_equal(
        df.iloc[:3].reset_index(drop=True), df2)
    expected_metadata = pd.DataFrame([
        dict(employee_id=1, department=1, name=1, wfh_stipend=0)
    ])
    pd.testing.assert_frame_equal(
        md_dict['metadata'], expected_metadata)


def test_reject_on_str_content(customers):
    df = pd.DataFrame(**customers())
    df2, md_dict = cl.reject_on_str_content(df, dict(foreign_key='25'))
    pd.testing.assert_frame_equal(df[1:].reset_index(drop=True), df2)
    expected_metadata = pd.DataFrame([
        dict(id=1, fname=1, lname=1, foreign_key=1)
    ])
    pd.testing.assert_frame_equal(
        md_dict['metadata'], expected_metadata)


def cleanse_redundancies():
    df = pd.DataFrame([
        dict(a=1, b=1, c=1),
        dict(a=2, b=3, c=2),
        dict(a=3, b=nan, c=nan),
    ])
    expected = pd.DataFrame([
        dict(a=1, b=nan, c=nan),
        dict(a=2, b=3, c=nan),
        dict(a=3, b=nan, c=nan),
    ])
    df, md_dict = cl.cleanse_redundancies(df, dict(a=('b', 'c')))
    pd.testing.assert_frame_equal(df, expected)
    expected_metadata = pd.DataFrame([
        dict(a=0, b=1, c=2)
    ])
    pd.testing.assert_frame_equal(
        md_dict['metadata'], expected_metadata)


def test_cleanse_typos(needs_cleanse_typos):
    df = pd.DataFrame(**needs_cleanse_typos)
    df2, md_dict = cl.cleanse_typos(
        df,
        dict(
            attr1=dict(cu='copper'),
            attr2=gd.CleaningGuide((('sm', 's'), 'small')))
    )
    pd.testing.assert_frame_equal(df, df2)
    expected_metadata = pd.DataFrame([
        dict(id=0, name=0, price=0, cost=0, upc=0, attr1=1, attr2=2,
             attr3=0, attr4=0, attr5=0)
    ])
    pd.testing.assert_frame_equal(
        md_dict['metadata'], expected_metadata)


def test_convert_types(customers, products):
    df = pd.DataFrame(**customers())
    df2, md_dict = cl.convert_types(
        df, {'id': int, 'foreign_key': e.ZeroNumeric})
    pd.testing.assert_frame_equal(df2, pd.DataFrame(**customers(int)))
    assert list(df2.dtypes) == ['int64', 'O', 'O', 'O']
    expected_metadata = pd.DataFrame([
        dict(id=4, fname=0, lname=0, foreign_key=4)
    ])
    pd.testing.assert_frame_equal(
        md_dict['metadata'], expected_metadata)

    df = pd.DataFrame(**products)
    df, md_dict = cl.convert_types(
        df, dict(price=int))
    pd.testing.assert_series_equal(
        df['price'], pd.Series([8, 9, 1, 5], name='price'))


def test_redistribute():
    df = pd.DataFrame([
        dict(a='red', b=nan),
        dict(a='L', b='blue'),
        dict(a='S', b=nan),
        dict(a='yellow', b=1),
        dict(a=123, b='x'),
    ])
    expected = pd.DataFrame([
        dict(a=nan, b='red'),
        dict(a='L', b='blue'),
        dict(a='S', b=nan),
        dict(a=nan, b=1),
        dict(a=123, b='x'),
    ])
    df2, md_dict = cl.redistribute(
        df.copy(), redistribution_guides=dict(
            a=gd.RedistributionGuide(
                'red', 'yellow', destination='b')
        ))
    pd.testing.assert_frame_equal(df2, expected)
    expected_metadata = pd.DataFrame([
        dict(a=2, b=1)
    ])
    pd.testing.assert_frame_equal(
        md_dict['metadata'], expected_metadata)

    expected = pd.DataFrame([
        dict(a=nan, b='red'),
        dict(a='L', b='blue'),
        dict(a='S', b=nan),
        dict(a=nan, b='yellow'),
        dict(a=nan, b=123),
    ])
    df2, md_dict = cl.redistribute(
        df.copy(), redistribution_guides=dict(
            a=gd.RedistributionGuide(
                'red', 'yellow', '123', destination='b', mode='overwrite')
        ))
    pd.testing.assert_frame_equal(df2, expected)
    expected_metadata = pd.DataFrame([
        dict(a=3, b=3)
    ])
    pd.testing.assert_frame_equal(
        md_dict['metadata'], expected_metadata)

    expected = pd.DataFrame([
        dict(a=nan, b='red'),
        dict(a='L', b='blue'),
        dict(a='S', b=nan),
        dict(a=nan, b='1 yellow'),
        dict(a=nan, b='x 123'),
    ])
    df2, md_dict = cl.redistribute(
        df.copy(), redistribution_guides=dict(
            a=gd.RedistributionGuide(
                'red', 'yellow', '123', destination='b', mode='append')
        ))
    pd.testing.assert_frame_equal(df2, expected)
    expected_metadata = pd.DataFrame([
        dict(a=3, b=3)
    ])
    pd.testing.assert_frame_equal(
        md_dict['metadata'], expected_metadata)
