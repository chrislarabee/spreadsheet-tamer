from numpy import nan
import pandas as pd

from datagenius.genius import GeniusAccessor
import datagenius.lib.preprocess as pp
import datagenius.util as u


def test_normalize_whitespace(gaps_totals):
    df = pd.DataFrame(
        [
            dict(a="a good string", b=" a  bad   string  ", c=nan),
            dict(a="     what       even     ", b=nan, c=123),
        ]
    )
    expected = pd.DataFrame(
        [
            dict(a="a good string", b="a bad string", c=nan),
            dict(a="what even", b=nan, c=123),
        ]
    )
    df, md_dict = pp.normalize_whitespace(df)
    pd.testing.assert_frame_equal(df, expected)
    assert md_dict["metadata"].iloc[0]["a"] == 1
    assert md_dict["metadata"].iloc[0]["b"] == 1
    assert md_dict["metadata"].iloc[0]["c"] == 0

    g = gaps_totals(False, False)
    df = pd.DataFrame(g[1:], columns=g[0])
    expected = pd.DataFrame(g[1:], columns=g[0])
    df, md_dict = pp.normalize_whitespace(df)
    pd.testing.assert_frame_equal(df, expected)
