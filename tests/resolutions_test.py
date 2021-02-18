import pytest
import pandas as pd
from numpy import nan

import tamer.resolutions as r


class TestCompleteClusters:
    def test_that_it_works_with_string_values(self, needs_extrapolation, employees):
        df = pd.DataFrame(**needs_extrapolation)
        df, md = r.complete_clusters(df, "department")
        pd.testing.assert_frame_equal(df, pd.DataFrame(**employees))
        expected_metadata = pd.DataFrame([dict(department=2)])
        pd.testing.assert_frame_equal(
            md["metadata"], expected_metadata, check_dtype=False
        )

    def test_that_it_works_with_numeric_values(self):
        df = pd.DataFrame(
            [
                dict(a=1, b=2, c=3),
                dict(a=nan, b=nan, c=nan),
                dict(a=1, b=nan, c=4),
                dict(a=nan, b=nan, c=nan),
            ]
        )
        expected = pd.DataFrame(
            [
                dict(a=1.0, b=2.0, c=3.0),
                dict(a=1.0, b=2.0, c=3.0),
                dict(a=1.0, b=2.0, c=4.0),
                dict(a=1.0, b=2.0, c=4.0),
            ]
        )
        df, md = r.complete_clusters(df, "a", "b", "c")
        pd.testing.assert_frame_equal(df, expected)
        expected_metadata = pd.DataFrame([dict(a=2, b=3, c=2)])
        pd.testing.assert_frame_equal(
            md["metadata"], expected_metadata, check_dtype=False
        )


class TestFillnaShift:
    @pytest.fixture
    def sample_df(self):
        return pd.DataFrame(
            [
                dict(a=1, b=nan, c=nan, d=2),
                dict(a=nan, b=3, c=4, d=nan),
                dict(a=nan, b=nan, c=5, d=nan),
            ]
        )

    def test_that_it_works_with_simple_ordering(self, sample_df):
        expected = pd.DataFrame(
            [
                dict(a=1, b=2, c=nan, d=nan),
                dict(a=3, b=4, c=nan, d=nan),
                dict(a=5, b=nan, c=nan, d=nan),
            ]
        )
        df2 = r.fillna_shift(sample_df, "a", "b", "c", "d")
        pd.testing.assert_frame_equal(df2, expected, check_dtype=False)

    def test_that_it_works_with_rigthward_ordering(self, sample_df):
        expected = pd.DataFrame(
            [
                dict(a=nan, b=nan, c=1, d=2),
                dict(a=nan, b=nan, c=3, d=4),
                dict(a=nan, b=nan, c=nan, d=5),
            ]
        )
        df2 = r.fillna_shift(sample_df, "d", "c", "b", "a")
        pd.testing.assert_frame_equal(df2, expected, check_dtype=False)

    def test_that_it_works_with_arbitrary_ordering(self, sample_df):
        expected = pd.DataFrame(
            [
                dict(a=nan, b=2, c=1, d=nan),
                dict(a=nan, b=3, c=4, d=nan),
                dict(a=nan, b=5, c=nan, d=nan),
            ]
        )
        df2 = r.fillna_shift(sample_df, "b", "c", "d", "a")
        pd.testing.assert_frame_equal(df2, expected, check_dtype=False)

    def test_that_it_raises_expected_error_on_too_few_columns(self, sample_df):
        with pytest.raises(ValueError, match="Must supply at least 2 columns."):
            r.fillna_shift(sample_df, "a")


class TestRemoveRedundancies:
    def test_that_it_works_on_integers(self):
        df = pd.DataFrame(
            [
                dict(a=1, b=1, c=1),
                dict(a=2, b=3, c=2),
                dict(a=3, b=nan, c=nan),
            ]
        )
        expected = pd.DataFrame(
            [
                dict(a=1, b=nan, c=nan),
                dict(a=2, b=3, c=nan),
                dict(a=3, b=nan, c=nan),
            ]
        )
        df, md = r.remove_redundancies(df, dict(a=("b", "c")))
        pd.testing.assert_frame_equal(df, expected)
        expected_metadata = pd.DataFrame([dict(a=0, b=1, c=2)])
        pd.testing.assert_frame_equal(md["metadata"], expected_metadata)
