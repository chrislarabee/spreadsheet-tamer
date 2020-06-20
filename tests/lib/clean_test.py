import pandas as pd

import datagenius.lib.clean as cl


def test_de_cluster(needs_extrapolation, employees):
    df = pd.DataFrame(**needs_extrapolation)
    df, md_dict = cl.de_cluster(df, ['department'])
    pd.testing.assert_frame_equal(df, pd.DataFrame(**employees))
    expected_metadata = pd.DataFrame([
        dict(department=2)
    ])
    pd.testing.assert_frame_equal(md_dict['metadata'], expected_metadata)
