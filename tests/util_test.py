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


def test_broadcast_suffix():
    assert u.broadcast_suffix(["x", "y", "z"], "_1") == ["x_1", "y_1", "z_1"]
    assert u.broadcast_suffix(pd.Index(["x", "y", "z"]), "_1") == ["x_1", "y_1", "z_1"]
    assert u.broadcast_suffix(pd.Series(["x", "y", "z"]), "_1") == ["x_1", "y_1", "z_1"]


def test_broadcast_type():
    assert u.broadcast_type(["1", "2", "3"], int) == [1, 2, 3]
    assert u.broadcast_type(["1", "0.5", "2"], u.isnumericplus) == [1, 0.5, 2]


def test_collect_by_keys():
    x = u.collect_by_keys({"a": 1, "b": 2, "c": 3, "d": 4}, "a", "c")
    assert x == {"a": 1, "c": 3}
    assert isinstance(x, dict) and not isinstance(x, od)
    x = u.collect_by_keys(od(e=5, f=6, g=7), "e", "f")
    assert x == od(e=5, f=6)
    assert type(x) == od and type(x) != dict


def test_count_true_str():
    assert u.count_true_str(["", "", ""]) == 0
    assert u.count_true_str(["a", "test", 1]) == 2
    assert u.count_true_str(pd.Series(["a", "test", 1])) == 2
    assert u.count_true_str(pd.Series([np.nan, "test", 1])) == 1


def test_enforce_uniques():
    assert u.enforce_uniques([1, 2, 3]) == [1, 2, 3]
    assert u.enforce_uniques(["x", "x", "y"]) == ["x", "x_1", "y"]
    assert u.enforce_uniques([1, 2, 2]) == [1, 2, "2_1"]


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


def test_gen_empty_md_df():
    expected = pd.DataFrame([dict(a=0, b=0, c=0)])
    pd.testing.assert_frame_equal(u.gen_empty_md_df(["a", "b", "c"]), expected)

    expected = pd.DataFrame([dict(a="x", b="x", c="x")])
    pd.testing.assert_frame_equal(u.gen_empty_md_df(["a", "b", "c"], "x"), expected)


def test_get_class_name():
    assert u.get_class_name("string") == "str"
    assert u.get_class_name(123) == "int"
    assert u.get_class_name(1.245) == "float"
    assert u.get_class_name(nan) == "nan"
    assert u.get_class_name([1, 2, 3]) == "list"
    assert u.get_class_name(dict(a=1, b=2, c=3)) == "dict"



def test_gwithin():
    assert u.gwithin([1, 2, 3], 1)
    assert u.gwithin([1, 2, 3], 1, 4)
    assert not u.gwithin([1, 2, 3], 4, 5)
    assert u.gwithin(["xyz", "a23"], r"[a-z]\d+")
    assert not u.gwithin(["xyz", "a23"], r"[a-z]\d[a-z]")
    assert u.gwithin(pd.Index(["unnamed_0", "unnamed_1"]), r"[Uu]nnamed:*[ _]\d")
    assert u.gwithin(pd.Index(["Unnamed: 0", "Unnamed: 1"]), r"[Uu]nnamed:*[ _]\d")
    assert u.gwithin(pd.Index(["Unnamed:_0", "Unnamed:_1"]), r"[Uu]nnamed:*[ _]\d")


def test_purge_gap_rows(gaps, gaps_totals):
    d = pd.DataFrame(gaps)
    d = u.purge_gap_rows(d)
    assert d.shape == (5, 4)
    d = pd.DataFrame(gaps_totals())
    d = u.purge_gap_rows(d)
    assert d.shape == (9, 3)


def test_standardize_header():
    header = pd.Index(
        [
            "Variant SKU",
            " Barcode  2 ",
            "Barcode  #3",
            "Barcode 3",
            "$ cost",
        ]
    )
    expected = [
        "variant_sku",
        "barcode_2",
        "barcode_3",
        "barcode_3_1",
        "cost",
    ]
    assert u.standardize_header(header) == (expected, list(header))

    header = pd.RangeIndex(0, 2, 1)
    assert u.standardize_header(header) == (["0", "1"], list(header))


def test_translate_null():
    assert pd.isna(u.translate_null(None))
    assert pd.isna(u.translate_null(nan))
    assert u.translate_null(nan, None) is None
    assert u.translate_null(None, None) is None
    assert u.translate_null("string") == "string"

    with pytest.raises(ValueError, match="must be numpy nan or None"):
        u.translate_null(1, int)


def test_tuplify():
    assert isinstance(u.tuplify("test"), tuple)
    assert u.tuplify("test") == ("test",)
    assert u.tuplify(None) is None
    assert u.tuplify(None, True) == (None,)
    assert u.tuplify([1, 2, 3]) == (1, 2, 3)
    assert u.tuplify({1, 2, 3}) == (1, 2, 3)
    assert u.tuplify({"a": 1, "b": 2}) == (("a", 1), ("b", 2))
    assert u.tuplify(1) == (1,)
    assert u.tuplify(1.23) == (1.23,)


def test_tuplify_iterable():
    assert u.tuplify_iterable([1, 2, 3]) == [(1,), (2,), (3,)]


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
