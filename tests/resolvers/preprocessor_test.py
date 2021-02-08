import pandas as pd
from numpy import nan

from tamer.resolvers.preprocessor import Preprocessor


class TestPreprocessor:
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

        def test_that_it_can_handle_a_dataframe_that_doesnt_need_a_purge(self, customers):
            df = pd.DataFrame(**customers())
            df = Preprocessor._purge_pre_header(df)
            assert df.shape == (4, 4)
