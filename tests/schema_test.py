from pathlib import Path

import pytest
import pandas as pd
from numpy import nan

from tamer import schema as sc


class TestValid:
    def test_that_it_can_be_valid(self):
        v = sc.Valid()
        assert v
        assert str(v) == "True"
        assert v.invalid_reasons == []

    def test_that_it_can_be_invalid(self):
        v = sc.Valid("violated rule")
        assert not v
        assert str(v) == "False"
        assert v.invalid_reasons == ["violated rule"]

    def test_that_pandas_series_can_handle_it(self):
        s = pd.Series([sc.Valid(), sc.Valid("violation"), sc.Valid("other violation")])
        expected = pd.Series(["True", "False", "False"])
        pd.testing.assert_series_equal(s.astype(str), expected)
        assert pd.Series([True, False, False]).equals(s.astype(bool))

    def test_that_two_valid_valid_objs_can_be_added(self):
        v1 = sc.Valid()
        v2 = sc.Valid()
        v1 += v2
        assert v1.invalid_reasons == []

    def test_that_two_valid_objs_can_be_added(self):
        v1 = sc.Valid()
        v2 = sc.Valid("violated rule")
        v1 += v2
        assert v1.invalid_reasons == ["violated rule"]

    def test_that_it_throws_an_error_on_attempting_to_add_non_valid_objs(self):
        v = sc.Valid()
        with pytest.raises(TypeError):
            v += 1  # type: ignore

    def test_that_it_can_be_added_in_pandas_series(self):
        s1 = pd.Series([sc.Valid(), sc.Valid("violation"), sc.Valid("other violation")])
        s2 = pd.Series([sc.Valid("violation"), sc.Valid(), sc.Valid("violation")])
        s1 += s2
        assert s1[0].invalid_reasons == ["violation"]
        assert s1[1].invalid_reasons == ["violation"]
        assert s1[2].invalid_reasons == ["other violation", "violation"]

    def test_that_it_can_be_used_in_equalities(self):
        v = sc.Valid()
        assert v == True
        assert v != False
        v1 = sc.Valid("violation")
        v2 = sc.Valid("violation")
        assert v1 == v2
        assert v != v1


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
            assert v.invalid_reasons == [
                "Column a value is not data type <class 'int'>"
            ]

        def test_that_it_works_with_null_values(self):
            c = sc.Column(int, "a")
            assert c.evaluate(1)
            assert c.evaluate(nan)
            c = sc.Column(int, "a", required=True)
            v = c.evaluate(nan)
            assert not v
            assert v.invalid_reasons == ["Column a is required"]

        def test_that_it_works_with_valid_values(self):
            c = sc.Column(int, "a", valid_values=[1, 2, 3])
            assert c.evaluate(1)
            v = c.evaluate(4)
            assert not v
            assert v.invalid_reasons == ["<4> is not a valid value for Column a"]
            c = sc.Column(str, "a", valid_values=["a", "b", "c"])
            assert c.evaluate("a")
            v = c.evaluate("bar")
            assert not v
            assert v.invalid_reasons == ["<bar> is not a valid value for Column a"]

        def test_that_it_works_with_invalid_values(self):
            c = sc.Column(int, "a", invalid_values=[1, 2, 3])
            assert c.evaluate(4)
            v = c.evaluate(1)
            assert not v
            assert v.invalid_reasons == ["<1> is not a valid value for Column a"]
            c = sc.Column(str, "a", invalid_values=["a", "b", "c"])
            assert c.evaluate("d")
            v = c.evaluate("a")
            assert not v
            assert v.invalid_reasons == ["<a> is not a valid value for Column a"]

        def test_that_it_works_with_valid_patterns(self):
            c = sc.Column(str, "a", valid_patterns=[r"^S$", r"^M$", r"^L$", r"^X+L$"])
            assert c.evaluate("S")
            v = c.evaluate("Small")
            assert not v
            assert v.invalid_reasons == [
                "<Small> does not match valid patterns for Column a"
            ]
            assert c.evaluate("XL")
            assert c.evaluate("XXXL")
            v = c.evaluate("-XL")
            assert not v
            assert v.invalid_reasons == [
                "<-XL> does not match valid patterns for Column a"
            ]

        def test_that_it_works_with_invalid_patterns(self):
            c = sc.Column(str, "a", invalid_patterns=[r"\d", r"\s"])
            assert c.evaluate("latte")
            v = c.evaluate("no3lle")
            assert not v
            assert v.invalid_reasons == [
                "<no3lle> matches invalid pattern <\d> for Column a"  # type: ignore
            ]
            v = c.evaluate("what now")
            assert not v
            assert v.invalid_reasons == [
                "<what now> matches invalid pattern <\s> for Column a"  # type: ignore
            ]

        def test_that_it_works_with_valid_values_and_patterns(self):
            c = sc.Column(
                str, "a", valid_values=["abc", "def"], valid_patterns=[r"\w+\d"]
            )
            assert c.evaluate("abc")
            assert c.evaluate("hamster1")
            v = c.evaluate("abc ")
            assert not v
            assert v.invalid_reasons == [
                "<abc > does not match valid patterns for Column a"
            ]
            v = c.evaluate("1def")
            assert not v
            assert v.invalid_reasons == [
                "<1def> does not match valid patterns for Column a"
            ]

        def test_that_it_works_with_invalid_values_and_patterns(self):
            c = sc.Column(
                str, "a", invalid_values=["abc", "def"], invalid_patterns=[r"\d$"]
            )
            assert c.evaluate("xyz")
            assert c.evaluate("1xyz")
            v = c.evaluate("abc")
            assert not v
            assert v.invalid_reasons == ["<abc> is not a valid value for Column a"]
            v = c.evaluate("xyz1")
            assert not v
            assert v.invalid_reasons == [
                "<xyz1> matches invalid pattern <\d$> for Column a"  # type: ignore
            ]


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

    @pytest.fixture
    def sample_df(self):
        return pd.DataFrame(
            [
                ["foo", 1, "1 fish", nan],
                ["bar", 2, "2 fish", 50.0],
                ["bar", 3, "red fish", 100.0],
                ["eggs", 4, "blue fish", nan],
            ],
            columns=["a", "b", "c", "d"],
        )

    @pytest.fixture
    def sample_schema(self):
        return sc.Schema(
            a=sc.Column(str),
            b=sc.Column(int),
            c=sc.Column(str, invalid_patterns=[r"^\d", r"green"]),
        )

    class TestValidate:
        def test_that_it_can_handle_single_column_validation(
            self, sample_df, sample_schema
        ):
            expected_bools = pd.Series([False, False, True, True], name="row_valid")
            expected_reasons = pd.Series(
                [
                    ["<1 fish> matches invalid pattern <^\d> for Column c"],  # type: ignore
                    ["<2 fish> matches invalid pattern <^\d> for Column c"],  # type: ignore
                    [],
                    [],
                ],
                name="row_valid",
            )
            df = sample_schema.validate(sample_df)
            pd.testing.assert_series_equal(df["row_valid"].astype(bool), expected_bools)
            reasons = df["row_valid"].apply(lambda x: x.invalid_reasons)
            pd.testing.assert_series_equal(reasons, expected_reasons)

        def test_that_it_can_handle_multi_column_validation(self, sample_df):
            s = sc.Schema(
                a=sc.Column(str, invalid_values=["eggs"]),
                b=sc.Column(int, invalid_values=[1]),
                c=sc.Column(str, invalid_patterns=[r"^\d", r"green"]),
            )
            expected_bools = pd.Series([False, False, True, False], name="row_valid")
            expected_reasons = pd.Series(
                [
                    [
                        "<1> is not a valid value for Column b",
                        "<1 fish> matches invalid pattern <^\d> for Column c",  # type: ignore
                    ],
                    ["<2 fish> matches invalid pattern <^\d> for Column c"],  # type: ignore
                    [],
                    ["<eggs> is not a valid value for Column a"],
                ],
                name="row_valid",
            )
            df = s.validate(sample_df)
            pd.testing.assert_series_equal(df["row_valid"].astype(bool), expected_bools)
            reasons = df["row_valid"].apply(lambda x: x.invalid_reasons)
            pd.testing.assert_series_equal(reasons, expected_reasons)

        def test_that_it_can_handle_unique_value_constraints(self, sample_df):
            s = sc.Schema(a=sc.Column(str, unique=True))
            df = s.validate(sample_df)
            expected_bools = pd.Series([True, False, False, True], name="row_valid")
            expected_reasons = pd.Series(
                [
                    [],
                    ["Column a must be unique"],
                    ["Column a must be unique"],
                    [],
                ],
                name="row_valid",
            )
            pd.testing.assert_series_equal(df["row_valid"].astype(bool), expected_bools)
            reasons = df["row_valid"].apply(lambda x: x.invalid_reasons)
            pd.testing.assert_series_equal(reasons, expected_reasons)

        def test_that_it_can_handle_unique_value_constraints_and_valid_values(
            self, sample_df
        ):
            s = sc.Schema(a=sc.Column(str, unique=True, invalid_values=["bar", "eggs"]))
            df = s.validate(sample_df)
            expected_bools = pd.Series([True, False, False, False], name="row_valid")
            expected_reasons = pd.Series(
                [
                    [],
                    [
                        "Column a must be unique",
                        "<bar> is not a valid value for Column a",
                    ],
                    [
                        "Column a must be unique",
                        "<bar> is not a valid value for Column a",
                    ],
                    ["<eggs> is not a valid value for Column a"],
                ],
                name="row_valid",
            )
            pd.testing.assert_series_equal(df["row_valid"].astype(bool), expected_bools)
            reasons = df["row_valid"].apply(lambda x: x.invalid_reasons)
            pd.testing.assert_series_equal(reasons, expected_reasons)

        def test_that_it_can_handle_required_columns(self, sample_df):
            s = sc.Schema(d=sc.Column(float, required=True))
            df = s.validate(sample_df)
            expected_bools = pd.Series([False, True, True, False], name="row_valid")
            expected_reasons = pd.Series(
                [
                    ["Column d is required"],
                    [],
                    [],
                    ["Column d is required"],
                ],
                name="row_valid",
            )
            pd.testing.assert_series_equal(df["row_valid"].astype(bool), expected_bools)
            reasons = df["row_valid"].apply(lambda x: x.invalid_reasons)
            pd.testing.assert_series_equal(reasons, expected_reasons)

    class TestEnforceSchemaRules:
        def test_that_it_works_as_expected(self, sample_df, sample_schema):
            df, _ = sample_schema.enforce_schema_rules(sample_df)
            assert len(df) == 2
            pd.testing.assert_index_equal(df.index, pd.Index([2, 3]))
