from numpy import nan
import pandas as pd

import datagenius.element as e
import datagenius.lib.preprocess as pp


def test_purge_pre_header(gaps_totals, customers):
    d = e.Dataset(gaps_totals())
    assert d.shape == (11, 3)
    d = d.purge_gap_rows(d)
    d = pp.detect_header(d)
    d = pp.purge_pre_header(d)
    assert list(d.columns) == ['location', 'region', 'sales']
    assert d.shape == (6, 3)
    assert d.rejects == [
        ['Sales by Location Report', nan, nan],
        ['Grouping: Region', nan, nan],
    ]
    assert d.reject_ct == 2

    # Test a dataset that doesn't need a purge:
    d = e.Dataset(**customers())
    d = pp.purge_pre_header(d)
    assert d.shape == (4, 4)
    assert d.rejects == []


def test_detect_header(gaps):
    d = e.Dataset(gaps)
    assert d.header_idx is None
    d = pp.detect_header(d)
    assert list(d.columns) == [
        'id', 'fname', 'lname', 'foreign_key'
    ]
    assert d.shape == (9, 4)
    assert d.header_idx == 4

    man_header = ['A', 'B', 'C', 'D']
    d = e.Dataset(gaps)
    assert d.header_idx is None
    d = pp.detect_header(d, manual_header=man_header)
    assert list(d.columns) == man_header
    assert d.header_idx is None

    # Test headerless Dataset:
    d = e.Dataset([[1, 2, 3], [4, 5, 6]])
    d = pp.detect_header(d)
    assert list(d.columns) == [0, 1, 2]
    assert d.header_idx is None


def test_normalize_whitespace(gaps_totals):
    d = e.Dataset([
        dict(a='a good string', b=' a  bad   string  ', c=nan),
        dict(a='     what       even     ', b=nan, c=123)
    ])
    expected = e.Dataset([
        dict(a='a good string', b='a bad string', c=nan),
        dict(a='what even', b=nan, c=123)
    ])
    d = pp.normalize_whitespace(d)
    pd.testing.assert_frame_equal(d, expected)

    g = gaps_totals(False, False)
    d = e.Dataset(g[1:], columns=g[0])
    expected = e.Dataset(g[1:], columns=g[0])
    d = pp.normalize_whitespace(d)
    pd.testing.assert_frame_equal(d, expected)
