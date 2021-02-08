from collections import OrderedDict as od
import string

import pytest
import pandas as pd
import numpy as np
from numpy import nan

import datagenius.util as u
import datagenius.element as e


def test_transmutation():
    @u.transmutation
    def _func1(x):
        return x

    assert _func1.stage == "_no_stage"
    assert _func1.args == ["x"]

    @u.transmutation("rejects", stage="preprocess")
    def _func2(x):
        return x

    assert _func2.stage == "preprocess"

    @u.transmutation(stage="a custom stage")
    def _func3(x):
        return x

    assert _func3.stage == "a_custom_stage"


def test_align_args():
    assert u.align_args(lambda x, y: x + y, kwargs=dict(x=1, y=3)) == dict(x=1, y=3)
    assert u.align_args(lambda x, y: x + y, dict(x=1, y=3, z=2), "y") == dict(x=1)
    assert u.align_args(lambda x, y: x + y, dict(x=1), ["y", "z"]) == dict(x=1)

    @u.transmutation
    def _func(x, y, z):
        return x, y, z

    assert u.align_args(_func, dict(x=1, y=2, z=3)) == dict(x=1, y=2, z=3)


def test_gen_alpha_keys():
    assert u.gen_alpha_keys(5) == ["A", "B", "C", "D", "E"]
    assert u.gen_alpha_keys(26) == list(string.ascii_uppercase)
    assert u.gen_alpha_keys(27) == [*string.ascii_uppercase, "AA"]
    assert u.gen_alpha_keys(28) == [*string.ascii_uppercase, "AA", "AB"]
    assert u.gen_alpha_keys(53) == [
        *string.ascii_uppercase,
        *["A" + a for a in string.ascii_uppercase],
        "BA",
    ]



def test_purge_gap_rows(gaps, gaps_totals):
    d = pd.DataFrame(gaps)
    d = u.purge_gap_rows(d)
    assert d.shape == (5, 4)
    d = pd.DataFrame(gaps_totals())
    d = u.purge_gap_rows(d)
    assert d.shape == (9, 3)


def test_translate_null():
    assert pd.isna(u.translate_null(None))
    assert pd.isna(u.translate_null(nan))
    assert u.translate_null(nan, None) is None
    assert u.translate_null(None, None) is None
    assert u.translate_null("string") == "string"

    with pytest.raises(ValueError, match="must be numpy nan or None"):
        u.translate_null(1, int)


def test_validate_attr():
    df = pd.DataFrame([[1, 2, 3], [4, 5, 6]])
    assert u.validate_attr(df, "shape", (2, 3))
    assert u.validate_attr(df, "shape")
    assert not u.validate_attr(df, "gibberish", "nonsense")


def test_gsheet_range_formula_basic(df_for_formulas):
    df = pd.DataFrame(df_for_formulas)
    df = u.gsheet_range_formula(df)
    expected = pd.Series(["=SUM(A2:C2)", "=SUM(A3:C3)", "=SUM(A4:C4)"], name="sum")
    pd.testing.assert_series_equal(df["sum"], expected)

    df = pd.DataFrame(df_for_formulas)
    df = u.gsheet_range_formula(df, axis=1)
    expected = pd.Series(
        ["=SUM(A2:A4)", "=SUM(B2:B4)", "=SUM(C2:C4)"],
        name=3,
        index=["col1", "col2", "col3"],
    )
    pd.testing.assert_series_equal(df.loc[3, :], expected)


def test_gsheet_range_formula_label_range(df_for_formulas):
    df = pd.DataFrame(df_for_formulas)
    df = u.gsheet_range_formula(df, label_range=(1, 2))
    expected = pd.Series([nan, "=SUM(A3:C3)", "=SUM(A4:C4)"], name="sum")
    pd.testing.assert_series_equal(df["sum"], expected)

    df = pd.DataFrame(df_for_formulas)
    df = u.gsheet_range_formula(df, axis=1, label_range=("col1", "col2"))
    expected = pd.Series(
        ["=SUM(A2:A4)", "=SUM(B2:B4)", nan], name=3, index=["col1", "col2", "col3"]
    )
    pd.testing.assert_series_equal(df.loc[3, :], expected)


def test_gsheet_range_formula_new_label(df_for_formulas):
    df = pd.DataFrame(df_for_formulas)
    df = u.gsheet_range_formula(df, new_label="total")
    expected = pd.Series(["=SUM(A2:C2)", "=SUM(A3:C3)", "=SUM(A4:C4)"], name="total")
    pd.testing.assert_series_equal(df["total"], expected)

    df = pd.DataFrame(df_for_formulas)
    df = u.gsheet_range_formula(df, axis=1, new_label=0)
    expected = pd.Series(
        ["=SUM(A3:A5)", "=SUM(B3:B5)", "=SUM(C3:C5)"],
        name=0,
        index=["col1", "col2", "col3"],
    )
    pd.testing.assert_series_equal(df.loc[0, :], expected)


def test_gsheet_range_formula_col_order(df_for_formulas):
    df = pd.DataFrame(df_for_formulas)
    df = u.gsheet_range_formula(df, col_order=["col3", "col1", "col2"])
    expected = pd.Series(["=SUM(A2:C2)", "=SUM(A3:C3)", "=SUM(A4:C4)"], name="sum")
    pd.testing.assert_series_equal(df["sum"], expected)

    df = pd.DataFrame(df_for_formulas)
    df = u.gsheet_range_formula(df, axis=1, col_order=["col3", "col1", "col2"])
    expected = pd.Series(
        ["=SUM(B2:B4)", "=SUM(C2:C4)", "=SUM(A2:A4)"],
        name=3,
        index=["col1", "col2", "col3"],
    )
    pd.testing.assert_series_equal(df.loc[3, :], expected)


def test_gsheet_range_formula_f_range(df_for_formulas):
    df = pd.DataFrame(df_for_formulas)
    df = u.gsheet_range_formula(df, f_range=("col2", "col3"))
    expected = pd.Series(["=SUM(B2:C2)", "=SUM(B3:C3)", "=SUM(B4:C4)"], name="sum")
    pd.testing.assert_series_equal(df["sum"], expected)

    df = pd.DataFrame(df_for_formulas)
    df = u.gsheet_range_formula(df, axis=1, f_range=(1, 2))
    expected = pd.Series(
        ["=SUM(A3:A4)", "=SUM(B3:B4)", "=SUM(C3:C4)"],
        name=3,
        index=["col1", "col2", "col3"],
    )
    pd.testing.assert_series_equal(df.loc[3, :], expected)

    df = pd.DataFrame(df_for_formulas)
    df = u.gsheet_range_formula(df, axis=1, f_range=(1, None))
    expected = pd.Series(
        ["=SUM(A3:A)", "=SUM(B3:B)", "=SUM(C3:C)"],
        name=3,
        index=["col1", "col2", "col3"],
    )
    pd.testing.assert_series_equal(df.loc[3, :], expected)


def test_gsheet_range_formula_col_order_f_range(df_for_formulas):
    df = pd.DataFrame(df_for_formulas)
    df = u.gsheet_range_formula(
        df, f_range=("col1", "col2"), col_order=["col3", "col1", "col2"]
    )
    expected = pd.Series(["=SUM(B2:C2)", "=SUM(B3:C3)", "=SUM(B4:C4)"], name="sum")
    pd.testing.assert_series_equal(df["sum"], expected)

    df = pd.DataFrame(df_for_formulas)
    df = u.gsheet_range_formula(
        df, axis=1, f_range=(1, 2), col_order=["col3", "col1", "col2"]
    )
    expected = pd.Series(
        ["=SUM(B3:B4)", "=SUM(C3:C4)", "=SUM(A3:A4)"],
        name=3,
        index=["col1", "col2", "col3"],
    )
    pd.testing.assert_series_equal(df.loc[3, :], expected)
