import pandas as pd
from numpy import nan

import datagenius.lib.clean as cl


def test_de_cluster(needs_extrapolation, employees):
    df = pd.DataFrame(**needs_extrapolation)
    df, md_dict = cl.complete_clusters(df, ['department'])
    pd.testing.assert_frame_equal(df, pd.DataFrame(**employees))
    expected_metadata = pd.DataFrame([
        dict(department=2)
    ])
    pd.testing.assert_frame_equal(md_dict['metadata'], expected_metadata)


def test_reject_incomplete_rows(needs_cleanse_totals, sales):
    df = pd.DataFrame(**needs_cleanse_totals)
    df, md_dict = cl.reject_incomplete_rows(df, ['location', 'region'])
    pd.testing.assert_frame_equal(df, pd.DataFrame(**sales))
    expected_metadata = pd.DataFrame([
        dict(location=0, region=0, sales=2)
    ])
    pd.testing.assert_frame_equal(md_dict['metadata'], expected_metadata)
    expected_rejects = pd.DataFrame([
        dict(location=nan, region=nan, sales=800),
        dict(location=nan, region=nan, sales=1200),
    ], index=[2, 5])
    pd.testing.assert_frame_equal(md_dict['rejects'], expected_rejects,
                                  check_dtype=False)
