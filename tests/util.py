import datagenius.util as u


def test_non_null_count():
    assert u.non_null_count(['', '', '']) == 0
    assert u.non_null_count([1, '', '']) == 1
    assert u.non_null_count([1, 2, 3]) == 3