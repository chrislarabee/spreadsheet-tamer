import datagenius.element as e
import datagenius.lib.preprocess as pp


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
