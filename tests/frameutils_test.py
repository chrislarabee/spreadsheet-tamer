import pytest
import pandas as pd
from numpy import nan

from tamer import frameutils
from tamer.decorators import nullable


class TestComplexJoinRule:
    def test_that_its_subscriptable(self):
        r = frameutils.ComplexJoinRule("a", "b", "c", inexact=True)
        assert r.thresholds[0] == 0.9

    def test_that_output_works(self):
        r = frameutils.ComplexJoinRule("a", "b", "c", conditions=dict(c="x"))
        assert r.output() == (("a", "b", "c"), {"c": ("x",)})
        assert r.output("on", "thresholds") == (("a", "b", "c"), None)
        assert r.output("on") == ("a", "b", "c")


class TestComplexJoinDaemon:
    class TestPrepOns:
        def test_that_it_wraps_tuples_and_strings_in_tuples(self):
            assert frameutils.ComplexJoinDaemon._prep_ons(("a", "b")) == (("a", "b"),)
            assert frameutils.ComplexJoinDaemon._prep_ons("a") == ("a",)

    class TestPrepSuffixes:
        def test_that_it_works_with_no_suffixes_supplied(self):
            assert frameutils.ComplexJoinDaemon._prep_suffixes(2) == ("_A", "_B")

        def test_that_it_works_with_suffixes_supplied(self):
            assert frameutils.ComplexJoinDaemon._prep_suffixes(2, ("_x", "_y")) == ("_x", "_y")
            assert frameutils.ComplexJoinDaemon._prep_suffixes(1, "_x") == ("_x",)

        def test_that_it_raises_expected_error_with_improper_suffixes_length(self):
            with pytest.raises(ValueError, match="Suffix len=2, suffixes="):
                frameutils.ComplexJoinDaemon._prep_suffixes(3, ("_x", "_y"))


class TestAccrete:
    def test_that_it_works_with_a_single_group_by(self):
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
        df = frameutils.accrete(df, ["a"], ("b", "c"), ",")
        pd.testing.assert_frame_equal(df, expected)

    def test_that_it_works_with_multiple_group_by_values(self):
        df = pd.DataFrame(
            [
                dict(a="t", b="u", c=1),
                dict(a="t", b="u", c=2),
                dict(a="x", b="y", c=nan),
            ]
        )
        expected = pd.DataFrame(
            [
                dict(a="t", b="u", c="1.0 2.0"),
                dict(a="t", b="u", c="1.0 2.0"),
                dict(a="x", b="y", c=nan),
            ]
        )
        df = frameutils.accrete(df, ["a", "b"], "c")
        pd.testing.assert_frame_equal(df, expected)


class TestMultiapply:
    def test_that_it_works_with_broadcasting_a_simple_lambda_func(self):
        df = pd.DataFrame([dict(a=1, b=2, c=3), dict(a=4, b=5, c=6)])
        expected = pd.DataFrame([dict(a=2, b=2, c=6), dict(a=8, b=5, c=12)])
        df = frameutils.multiapply(df, "a", "c", func=lambda x: x * 2)
        pd.testing.assert_frame_equal(df, expected)

    @pytest.fixture
    def sample_df(self) -> pd.DataFrame:
        return pd.DataFrame(
            [
                [1, "abc", 8.5, "foo"],
                [2, "def", 4.0, "bar"],
                [3, "ghi", 9.50, "spam"],
                [4, "jkl", 5.2, "eggs"],
            ],
            columns=["a", "b", "c", "d"],
        )

    @pytest.fixture
    def expected_df(self) -> pd.DataFrame:
        return pd.DataFrame(
            [
                [1, "ABC", 8.5, "Foo"],
                [2, "DEF", 4.0, "Bar"],
                [3, "GHI", 9.50, "Spam"],
                [4, "JKL", 5.2, "Eggs"],
            ],
            columns=["a", "b", "c", "d"],
        )

    def test_that_it_works_with_column_func_pairs(self, sample_df, expected_df):
        df = frameutils.multiapply(sample_df, b=str.upper, d=str.title)
        pd.testing.assert_frame_equal(df, expected_df)

    def test_that_it_works_with_nullable_funcs(self):
        df = pd.DataFrame(
            [
                [1, "abc", 8.5, "foo"],
                [2, nan, 4.0, "bar"],
                [3, "def", 9.50, "spam"],
                [4, "ghi", 5.2, nan],
            ],
            columns=["a", "b", "c", "d"],
        )
        expected = pd.DataFrame(
            [
                [1, "Abc", 8.5, "Foo"],
                [2, nan, 4.0, "Bar"],
                [3, "Def", 9.50, "Spam"],
                [4, "Ghi", 5.2, nan],
            ],
            columns=["a", "b", "c", "d"],
        )

        @nullable
        def nullable_title(x):
            return x.title()

        df = frameutils.multiapply(df, "b", "d", func=nullable_title)
        pd.testing.assert_frame_equal(df, expected)

    def test_that_it_works_with_a_mix_of_pairs_and_broadcasting(self, sample_df):
        expected = pd.DataFrame(
            [
                [1, "ABC", 17.0, "FOO"],
                [2, "DEF", 8.0, "BAR"],
                [3, "GHI", 19.0, "SPAM"],
                [4, "JKL", 10.4, "EGGS"],
            ],
            columns=["a", "b", "c", "d"],
        )
        df = frameutils.multiapply(
            sample_df, "b", "d", func=str.upper, c=lambda x: x * 2
        )
        pd.testing.assert_frame_equal(df, expected)
