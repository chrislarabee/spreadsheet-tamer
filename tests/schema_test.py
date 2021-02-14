from pathlib import Path

import pytest
import pandas as pd

from tamer import schema as sc


class TestValid:
    def test_that_it_can_be_valid(self):
        v = sc.Valid()
        assert v
        assert str(v) == "valid"

    def test_that_it_can_be_invalid(self):
        v = sc.Valid("violated rule")
        assert not v
        assert str(v) == "violated rule"

    def test_that_pandas_series_can_handle_it(self):
        s = pd.Series([sc.Valid(), sc.Valid("violation"), sc.Valid("other violation")])
        expected = pd.Series(["valid", "violation", "other violation"])
        pd.testing.assert_series_equal(s.astype(str), expected)
        assert pd.Series([True, False, False]).equals(s.astype(bool))


class TestColumn:
    def test_that_it_handles_mixing_valid_and_invalid_as_expected(self):
        c = sc.Column(int, valid_values=[1, 2, 3], invalid_values=[4, 5, 6])
        assert c.valid_values == [1, 2, 3]
        assert c.invalid_values == []
        c = sc.Column(str, valid_patterns=[r"\D"], invalid_patterns=[r"\d"])
        assert c.valid_patterns == [r"\D"]
        assert c.invalid_patterns == []
        c = sc.Column(
            str, valid_values=["a"], invalid_patterns=[r"\d"], invalid_values=["a"]
        )
        assert c.valid_values == ["a"]
        assert c.invalid_patterns == []
        assert c.invalid_values == []
        c = sc.Column(
            str, valid_patterns=[r"\D"], invalid_patterns=[r"\d"], invalid_values=["a"]
        )
        assert c.valid_patterns == [r"\D"]
        assert c.invalid_patterns == []
        assert c.invalid_values == []

    class TestEvaluate:
        def test_that_it_works_with_only_data_type_constraint(self):
            c = sc.Column(int, "a")
            assert c.evaluate(1)
            v = c.evaluate("1")
            assert not v
            assert str(v) == "Column a value is not data type <class 'int'>"

        def test_that_it_works_with_valid_values(self):
            c = sc.Column(int, "a", valid_values=[1, 2, 3])
            assert c.evaluate(1)
            v = c.evaluate(4)
            assert not v
            assert str(v) == "<4> is not a valid value for Column a"
            c = sc.Column(str, "a", valid_values=["a", "b", "c"])
            assert c.evaluate("a")
            v = c.evaluate("bar")
            assert not v
            assert str(v) == "<bar> is not a valid value for Column a"

        def test_that_it_works_with_invalid_values(self):
            c = sc.Column(int, "a", invalid_values=[1, 2, 3])
            assert c.evaluate(4)
            v = c.evaluate(1)
            assert not v
            assert str(v) == "<1> is not a valid value for Column a"
            c = sc.Column(str, "a", invalid_values=["a", "b", "c"])
            assert c.evaluate("d")
            v = c.evaluate("a")
            assert not v
            assert str(v) == "<a> is not a valid value for Column a"

        def test_that_it_works_with_valid_patterns(self):
            c = sc.Column(str, "a", valid_patterns=[r"^S$", r"^M$", r"^L$", r"^X+L$"])
            assert c.evaluate("S")
            v = c.evaluate("Small")
            assert not v
            assert str(v) == "<Small> does not match valid patterns for Column a"
            assert c.evaluate("XL")
            assert c.evaluate("XXXL")
            v = c.evaluate("-XL")
            assert not v
            assert str(v) == "<-XL> does not match valid patterns for Column a"

        def test_that_it_works_with_invalid_patterns(self):
            c = sc.Column(str, "a", invalid_patterns=[r"\d", r"\s"])
            assert c.evaluate("latte")
            v = c.evaluate("no3lle")
            assert not v
            assert str(v) == (
                "<no3lle> matches invalid pattern <\d> for Column a"  # type: ignore
            )
            v = c.evaluate("what now")
            assert not v
            assert str(v) == (
                "<what now> matches invalid pattern <\s> for Column a"  # type: ignore
            )

        def test_that_it_works_with_valid_values_and_patterns(self):
            c = sc.Column(
                str, "a", valid_values=["abc", "def"], valid_patterns=[r"\w+\d"]
            )
            assert c.evaluate("abc")
            assert c.evaluate("hamster1")
            v = c.evaluate("abc ")
            assert not v
            assert str(v) == "<abc > does not match valid patterns for Column a"
            v = c.evaluate("1def")
            assert not v
            assert str(v) == "<1def> does not match valid patterns for Column a"

        def test_that_it_works_with_invalid_values_and_patterns(self):
            c = sc.Column(
                str, "a", invalid_values=["abc", "def"], invalid_patterns=[r"\d$"]
            )
            assert c.evaluate("xyz")
            assert c.evaluate("1xyz")
            v = c.evaluate("abc")
            assert not v
            assert str(v) == "<abc> is not a valid value for Column a"
            v = c.evaluate("xyz1")
            assert not v
            assert str(v) == (
                "<xyz1> matches invalid pattern <\d$> for Column a"  # type: ignore
            )


class TestSchema:
    def test_that_type_hints_cause_no_errors(self):
        s = sc.Schema(
            a=sc.Column(str),
            b=sc.Column(int),
        )

    def test_that_from_yaml_loads_successfully(self):
        s = sc.Schema.from_yaml(Path("tests/samples/schema.yml"))
        assert list(s.columns.keys()) == ["a", "b"]
        assert s.columns["a"].data_type == "int"
        assert s.columns["a"].valid_values == [1, 2, 3]
        assert s.columns["a"].invalid_values == []
        assert not s.columns["a"].required
        assert s.columns["a"].unique
        assert s.columns["b"].data_type == "str"
        assert s.columns["b"].valid_values == []
        assert s.columns["b"].invalid_values == []
        assert s.columns["b"].required
        assert not s.columns["b"].unique
