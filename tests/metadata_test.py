import pandas as pd

import datagenius.util as u
import datagenius.metadata as omd
import tamer.metadata as md


class TestGeniusMetadata:
    def test_track(self):
        @u.transmutation(stage="test_track")
        def tracked_func(df):
            return df, {"metadata": pd.DataFrame([dict(a=1, b=0, c=1)])}

        df = pd.DataFrame([dict(a=1, b=2, c=3), dict(a=4, b=5, c=6)])
        expected = pd.DataFrame(
            [dict(stage="test_track", transmutation="tracked_func", a=1, b=0, c=1)]
        )

        gmd = omd.GeniusMetadata()
        x, kwargs = gmd.track(tracked_func, df)
        assert kwargs == {}
        pd.testing.assert_frame_equal(x, df)
        pd.testing.assert_frame_equal(gmd.collected, expected, check_dtype=False)

        # Test rejects:
        @u.transmutation(stage="test_track")
        def tracked_func2(df):
            df.drop(1, inplace=True)
            return df, {
                "metadata": pd.DataFrame([dict(d=1)]),
                "rejects": pd.DataFrame([dict(a=4, b=5, c=6)]),
            }

        expected = pd.concat(
            (
                expected,
                pd.DataFrame(
                    [dict(stage="test_track", transmutation="tracked_func2", d=1)]
                ),
            )
        ).reset_index(drop=True)
        expected_rejects = pd.DataFrame([dict(a=4, b=5, c=6)])

        x, kwargs = gmd.track(tracked_func2, df)
        assert kwargs == {}
        pd.testing.assert_frame_equal(x, pd.DataFrame([dict(a=1, b=2, c=3)]))
        pd.testing.assert_frame_equal(gmd.collected, expected)
        pd.testing.assert_frame_equal(gmd.rejects, expected_rejects)

        # Test no stage and new_kwargs:
        @u.transmutation
        def tracked_func3(df):
            return df, {
                "metadata": pd.DataFrame([dict(e=4)]),
                "new_kwargs": dict(x=1, y=2),
            }

        expected = pd.concat(
            (
                expected,
                pd.DataFrame(
                    [dict(stage="_no_stage", transmutation="tracked_func3", e=4)]
                ),
            )
        ).reset_index(drop=True)

        x, kwargs = gmd.track(tracked_func3, df)
        assert kwargs == {"x": 1, "y": 2}
        pd.testing.assert_frame_equal(gmd.collected, expected, check_dtype=False)

    def test_combine(self):
        df1 = pd.DataFrame([dict(a="  val1   ", b="val2")])
        df1, m1 = df1.genius.preprocess()
        ex1 = pd.DataFrame(
            [
                dict(
                    stage="preprocess",
                    transmutation="normalize_whitespace",
                    a=1.0,
                    b=0.0,
                )
            ]
        )
        pd.testing.assert_frame_equal(m1.collected, ex1)
        df2 = pd.DataFrame([dict(a="val3     ", c="   val4      ")])
        df2, m2 = df2.genius.preprocess()
        ex2 = pd.DataFrame(
            [
                dict(
                    stage="preprocess",
                    transmutation="normalize_whitespace",
                    a=1.0,
                    c=1.0,
                )
            ]
        )
        pd.testing.assert_frame_equal(m2.collected, ex2)

        expected = pd.DataFrame(
            [
                dict(
                    stage="preprocess",
                    transmutation="normalize_whitespace",
                    a=2.0,
                    b=0.0,
                    c=1.0,
                )
            ]
        )
        m1.combine(m2)
        pd.testing.assert_frame_equal(m1.collected, expected)


class TestGenEmptyMDDF:
    def test_that_it_works_with_no_default_val(self):
        expected = pd.DataFrame([dict(a=0, b=0, c=0)])
        pd.testing.assert_frame_equal(md.gen_empty_md_df(["a", "b", "c"]), expected)

    def test_that_it_works_with_a_default_val(self):
        expected = pd.DataFrame([dict(a="x", b="x", c="x")])
        pd.testing.assert_frame_equal(md.gen_empty_md_df(["a", "b", "c"], "x"), expected)
