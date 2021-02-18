import pytest
import pandas as pd
from numpy import nan

from tamer import frameutils
from tamer.decorators import nullable


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
