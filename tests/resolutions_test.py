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
