import pandas as pd
from numpy import nan
import pytest

import datagenius.lib.clean as cl
import datagenius.lib.guides as gd
import datagenius.element as e


class TestCleaningGuide:
    def test_basics(self):
        cg = gd.CleaningGuide(("a", "x"), (("b", "c"), "y"), d="z")
        assert cg("a") == "x"
        assert cg("b") == "y"
        assert cg("c") == "y"
        assert cg("d") == "z"
        assert cg("e") == "e"

    def test_convert(self):
        cg = gd.CleaningGuide.convert(
            gd.CleaningGuide(("a", "x"), (("b", "c"), "y"), d="z")
        )
        assert cg("a") == "x"
        assert cg("b") == "y"
        assert cg("c") == "y"
        assert cg("d") == "z"
        assert cg("e") == "e"

        cg = gd.CleaningGuide.convert(dict(a="x", b="y", c="z"))
        assert cg("a") == "x"
        assert cg("b") == "y"
        assert cg("c") == "z"
        assert cg("e") == "e"

        with pytest.raises(ValueError, match="Invalid object=test, type=<class 'str'>"):
            cg = gd.CleaningGuide.convert("test")


def test_cleanse_typos(needs_cleanse_typos):
    df = pd.DataFrame(**needs_cleanse_typos)
    df2, md_dict = cl.cleanse_typos(
        df,
        dict(attr1=dict(cu="copper"), attr2=gd.CleaningGuide((("sm", "s"), "small"))),
    )
    pd.testing.assert_frame_equal(df, df2)
    expected_metadata = pd.DataFrame(
        [
            dict(
                id=0,
                name=0,
                price=0,
                cost=0,
                upc=0,
                attr1=1,
                attr2=2,
                attr3=0,
                attr4=0,
                attr5=0,
            )
        ]
    )
    pd.testing.assert_frame_equal(md_dict["metadata"], expected_metadata)


def test_convert_types(customers, products):
    df = pd.DataFrame(**customers())
    df2, md_dict = cl.convert_types(df, {"id": int, "foreign_key": e.ZeroNumeric})
    pd.testing.assert_frame_equal(df2, pd.DataFrame(**customers(int)))
    assert list(df2.dtypes) == ["int64", "O", "O", "O"]
    expected_metadata = pd.DataFrame([dict(id=4, fname=0, lname=0, foreign_key=4)])
    pd.testing.assert_frame_equal(md_dict["metadata"], expected_metadata)

    df = pd.DataFrame(**products)
    df, md_dict = cl.convert_types(df, dict(price=int))
    pd.testing.assert_series_equal(df["price"], pd.Series([8, 9, 1, 5], name="price"))


def test_redistribute():
    df = pd.DataFrame(
        [
            dict(a="red", b=nan),
            dict(a="L", b="blue"),
            dict(a="S", b=nan),
            dict(a="yellow", b=1),
            dict(a=123, b="x"),
        ]
    )
    expected = pd.DataFrame(
        [
            dict(a=nan, b="red"),
            dict(a="L", b="blue"),
            dict(a="S", b=nan),
            dict(a=nan, b=1),
            dict(a=123, b="x"),
        ]
    )
    df2, md_dict = cl.redistribute(
        df.copy(),
        redistribution_guides=dict(
            a=gd.RedistributionGuide("red", "yellow", destination="b")
        ),
    )
    pd.testing.assert_frame_equal(df2, expected)
    expected_metadata = pd.DataFrame([dict(a=2, b=1)])
    pd.testing.assert_frame_equal(md_dict["metadata"], expected_metadata)

    expected = pd.DataFrame(
        [
            dict(a=nan, b="red"),
            dict(a="L", b="blue"),
            dict(a="S", b=nan),
            dict(a=nan, b="yellow"),
            dict(a=nan, b=123),
        ]
    )
    df2, md_dict = cl.redistribute(
        df.copy(),
        redistribution_guides=dict(
            a=gd.RedistributionGuide(
                "red", "yellow", "123", destination="b", mode="overwrite"
            )
        ),
    )
    pd.testing.assert_frame_equal(df2, expected)
    expected_metadata = pd.DataFrame([dict(a=3, b=3)])
    pd.testing.assert_frame_equal(md_dict["metadata"], expected_metadata)

    expected = pd.DataFrame(
        [
            dict(a=nan, b="red"),
            dict(a="L", b="blue"),
            dict(a="S", b=nan),
            dict(a=nan, b="1 yellow"),
            dict(a=nan, b="x 123"),
        ]
    )
    df2, md_dict = cl.redistribute(
        df.copy(),
        redistribution_guides=dict(
            a=gd.RedistributionGuide(
                "red", "yellow", "123", destination="b", mode="append"
            )
        ),
    )
    pd.testing.assert_frame_equal(df2, expected)
    expected_metadata = pd.DataFrame([dict(a=3, b=3)])
    pd.testing.assert_frame_equal(md_dict["metadata"], expected_metadata)


def test_accrete():
    df = pd.DataFrame(
        [
            dict(a="t", b="u", c="v", d=1),
            dict(a="t", b="w", c=nan, d=2),
            dict(a="y", b="z", c="z", d=3),
        ]
    )
    expected = pd.DataFrame(
        [
            dict(a="t", b="u,w", c="v", d=1),
            dict(a="t", b="u,w", c="v", d=2),
            dict(a="y", b="z", c="z", d=3),
        ]
    )
    df2, md_dict = cl.accrete(df.copy(), ["a"], ("b", "c"), ",")
    pd.testing.assert_frame_equal(df2, expected)
    expected_metadata = pd.DataFrame([dict(a=0, b=2, c=2, d=0)])
    pd.testing.assert_frame_equal(md_dict["metadata"], expected_metadata)

    # Test multiple group_by:
    df = pd.DataFrame(
        [dict(a="t", b="u", c=1), dict(a="t", b="u", c=2), dict(a="x", b="y", c=nan)]
    )
    expected = pd.DataFrame(
        [
            dict(a="t", b="u", c="1.0 2.0"),
            dict(a="t", b="u", c="1.0 2.0"),
            dict(a="x", b="y", c=nan),
        ]
    )
    df2, md_dict = cl.accrete(df.copy(), ["a", "b"], "c")
    pd.testing.assert_frame_equal(df2, expected)
    expected_metadata = pd.DataFrame([dict(a=0, b=0, c=2)])
    pd.testing.assert_frame_equal(md_dict["metadata"], expected_metadata)
