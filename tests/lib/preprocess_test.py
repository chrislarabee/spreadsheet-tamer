import datagenius.element as e
import datagenius.lib.preprocess as pp


def test_purge_pre_header(gaps_totals, customers):
    d = e.Dataset(gaps_totals())
    d = d.pipe(pp.detect_header).pipe(pp.purge_pre_header)
    assert d.shape == (6, 3)
    assert d.rejects == [
        ['Sales by Location Report', '', ''],
        ['Grouping: Region', '', ''],
        ['', '', ''],
        ['', '', '']
    ]

    d = e.Dataset(**customers())
    d = d.pipe(pp.detect_header).pipe(pp.purge_pre_header)
    assert d.shape == (4, 4)
    assert d.rejects == []


def test_detect_header(gaps):
    d = e.Dataset(gaps)
    d = d.pipe(pp.detect_header)
    assert list(d.columns) == [
        'id', 'fname', 'lname', 'foreign_key'
    ]
    assert d.shape == (9, 4)
    assert d.meta_data.header_idx == 4

    man_header = ['A', 'B', 'C', 'D']
    d = e.Dataset(gaps)
    d = d.pipe(
        pp.detect_header,
        manual_header=man_header)
    assert list(d.columns) == man_header
    assert d.meta_data.header_idx is None

    # Test headerless Dataset:
    d = e.Dataset([[1, 2, 3], [4, 5, 6]])
    d = d.pipe(pp.detect_header)
    assert list(d.columns) == [0, 1, 2]
    assert d.meta_data.header_idx is None
