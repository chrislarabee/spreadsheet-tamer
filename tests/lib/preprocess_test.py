from numpy import nan
import pandas as pd

from datagenius.genius import GeniusAccessor
import datagenius.lib.preprocess as pp
import datagenius.util as u


def test_purge_pre_header(gaps_totals, customers):
    df = pd.DataFrame(gaps_totals())
    assert df.shape == (11, 3)
    df = u.purge_gap_rows(df)
    df, h = pp.detect_header(df)
    df, metadata = pp.purge_pre_header(df, h['new_kwargs']['header_idx'])
    assert list(df.columns) == ['location', 'region', 'sales']
    assert df.shape == (6, 3)
    expected = pd.DataFrame([
        ['Sales by Location Report', nan, nan],
        ['Grouping: Region', nan, nan],
    ], columns=['location', 'region', 'sales'])
    pd.testing.assert_frame_equal(metadata['rejects'], expected,
                                  check_dtype=False)
    expected = pd.DataFrame([dict(location=2, region=0, sales=0)])
    pd.testing.assert_frame_equal(metadata['metadata'], expected)

    # Test a DataFrame that doesn't need a purge:
    df = pd.DataFrame(**customers())
    df = pp.purge_pre_header(df)
    assert df.shape == (4, 4)


def test_detect_header(gaps):
    df = pd.DataFrame(gaps)
    df, metadata = pp.detect_header(df)
    assert list(df.columns) == [
        'id', 'fname', 'lname', 'foreign_key'
    ]
    assert df.shape == (9, 4)
    assert metadata['new_kwargs'] == {'header_idx': 4}

    man_header = ['A', 'B', 'C', 'df']
    df = pd.DataFrame(gaps)
    df, metadata = pp.detect_header(df, manual_header=man_header)
    assert list(df.columns) == ['a', 'b', 'c', 'df']
    assert metadata['new_kwargs'] == {'header_idx': None}
    assert metadata['orig_header'] == man_header

    # Test headerless Dataset:
    df = pd.DataFrame([[1, 2, 3], [4, 5, 6]])
    df, metadata = pp.detect_header(df)
    assert list(df.columns) == [0, 1, 2]
    assert metadata['new_kwargs'] == {'header_idx': None}
    assert metadata['orig_header'] == []


def test_normalize_whitespace(gaps_totals):
    df = pd.DataFrame([
        dict(a='a good string', b=' a  bad   string  ', c=nan),
        dict(a='     what       even     ', b=nan, c=123)
    ])
    expected = pd.DataFrame([
        dict(a='a good string', b='a bad string', c=nan),
        dict(a='what even', b=nan, c=123)
    ])
    df, md_dict = pp.normalize_whitespace(df)
    pd.testing.assert_frame_equal(df, expected)
    assert md_dict['metadata'].iloc[0]['a'] == 1
    assert md_dict['metadata'].iloc[0]['b'] == 1
    assert md_dict['metadata'].iloc[0]['c'] == 0

    g = gaps_totals(False, False)
    df = pd.DataFrame(g[1:], columns=g[0])
    expected = pd.DataFrame(g[1:], columns=g[0])
    df, md_dict = pp.normalize_whitespace(df)
    pd.testing.assert_frame_equal(df, expected)
