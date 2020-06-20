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
        expected = pd.DataFrame([dict(
            stage='preprocess', transmutation='tracked_func',
            a=1, b=0, c=1)]
        )

        gmd = md.GeniusMetadata()
        x, kwargs = gmd.track(tracked_func, df)
        assert kwargs == {}
        pd.testing.assert_frame_equal(x, df)
        pd.testing.assert_frame_equal(
            gmd.collected, expected, check_dtype=False
        )

        # Test rejects:
        @u.transmutation(stage='preprocess')
        def tracked_func2(df):
            df.drop(1, inplace=True)
            return df, {'metadata': pd.DataFrame([dict(d=1)]),
                        'rejects': pd.DataFrame([dict(a=4, b=5, c=6)])}

        expected = pd.concat((expected, pd.DataFrame([
            dict(stage='preprocess', transmutation='tracked_func2', d=1)]))
        ).reset_index(drop=True)
        expected_rejects = pd.DataFrame([dict(a=4, b=5, c=6)])

        x, kwargs = gmd.track(tracked_func2, df)
        assert kwargs == {}
        pd.testing.assert_frame_equal(
            x, pd.DataFrame([dict(a=1, b=2, c=3)]))
        pd.testing.assert_frame_equal(
            gmd.collected, expected)
        pd.testing.assert_frame_equal(gmd.rejects, expected_rejects)

        # Test no stage and new_kwargs:
        @u.transmutation
        def tracked_func3(df):
            return df, {'metadata': pd.DataFrame([dict(e=4)]),
                        'new_kwargs': dict(x=1, y=2)}

        expected = pd.concat((expected, pd.DataFrame([
            dict(stage='_no_stage', transmutation='tracked_func3', e=4)]))
        ).reset_index(drop=True)

        x, kwargs = gmd.track(tracked_func3, df)
        assert kwargs == {'x': 1, 'y': 2}
        pd.testing.assert_frame_equal(
            gmd.collected, expected, check_dtype=False)
