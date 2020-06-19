import pandas as pd
from numpy import nan

import datagenius.util as u
import datagenius.metadata as md


class TestGeniusMetadata:
    def test_track(self):
        @u.transmutation(stage='preprocess')
        def tracked_func(df):
            return df, {'metadata': pd.DataFrame([dict(a=1, b=0, c=1)])}

        df = pd.DataFrame([
            dict(a=1, b=2, c=3),
            dict(a=4, b=5, c=6)
        ])
        expected = pd.DataFrame([dict(a=1, b=0, c=1)])

        gmd = md.GeniusMetadata()
        x = gmd.track(tracked_func, df)
        pd.testing.assert_frame_equal(x, df)
        assert list(gmd.transmutations.keys()) == ['tracked_func']
        pd.testing.assert_frame_equal(
            gmd.transmutations['tracked_func'], expected)
        pd.testing.assert_frame_equal(
            gmd.stages['preprocess'], expected
        )

        @u.transmutation(stage='preprocess')
        def tracked_func2(df):
            df.drop(1, inplace=True)
            return df, {'metadata': pd.DataFrame([dict(d=1)]),
                        'rejects': pd.DataFrame([dict(a=4, b=5, c=6)])}

        expected = pd.DataFrame([
            dict(a=1, b=0, c=1, d=nan),
            dict(a=nan, b=nan, c=nan, d=1)
        ])
        expected_rejects = pd.DataFrame([dict(a=4, b=5, c=6)])

        x = gmd.track(tracked_func2, df)
        pd.testing.assert_frame_equal(
            x, pd.DataFrame([dict(a=1, b=2, c=3)]))
        assert list(gmd.transmutations.keys()) == [
            'tracked_func', 'tracked_func2']
        pd.testing.assert_frame_equal(
            gmd.transmutations['tracked_func2'], pd.DataFrame([dict(d=1)])
        )
        pd.testing.assert_frame_equal(
            gmd.stages['preprocess'], expected
        )
        pd.testing.assert_frame_equal(gmd.rejects, expected_rejects)
