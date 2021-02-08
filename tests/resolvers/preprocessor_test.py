import pandas as pd
from numpy import nan

from tamer.resolvers.preprocessor import Preprocessor
from tamer.header import Header


class TestPreprocessor:
    class TestDetectHeader:
        def test_detect_header(self, gaps):
            df = pd.DataFrame(gaps)
            df, header_idx = Preprocessor._detect_header(df)
            assert list(df.columns) == ["id", "fname", "lname", "foreign_key"]
            assert df.shape == (9, 4)
            assert header_idx == 4

        def test_that_it_works_with_a_manual_header(self, gaps):
            man_header = Header("A", "B", "C", "df")
            df = pd.DataFrame(gaps)
            df, header_idx = Preprocessor._detect_header(df, manual_header=man_header)
            assert list(df.columns) == ["a", "b", "c", "df"]
            assert header_idx is None

        def test_that_it_works_with_a_headerless_dataframe(self):
            df = pd.DataFrame([[1, 2, 3], [4, 5, 6]])
            df, header_idx = Preprocessor._detect_header(df)
            assert list(df.columns) == [0, 1, 2]
            assert header_idx is None

    class TestPurgePreHeader:
        def test_that_it_can_purge_pre_header(self, gaps_totals):
            df = pd.DataFrame(gaps_totals())
            expected = pd.DataFrame(df.iloc[:4].values.tolist(), columns=df.columns)
            assert df.shape == (11, 3)
            df, md = Preprocessor._purge_pre_header(df, 4)
            assert df.shape == (7, 3)
            pd.testing.assert_frame_equal(md["rejects"], expected, check_dtype=False)
            expected = pd.DataFrame([{0: 2, 1: 0, 2: 0}])
            pd.testing.assert_frame_equal(md["metadata"], expected)

        def test_that_it_can_handle_a_dataframe_that_doesnt_need_a_purge(
            self, customers
        ):
            df = pd.DataFrame(**customers())
            df = Preprocessor._purge_pre_header(df)
            assert df.shape == (4, 4)
