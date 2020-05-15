import datagenius.util as u


def test_non_null_count():
    assert u.non_null_count(['', '', '']) == 0
    assert u.non_null_count([1, '', '']) == 1
    assert u.non_null_count([1, 2, 3]) == 3


def test_true_str_count():
    assert u.true_str_count(['', '', '']) == 0
    assert u.true_str_count(['a', 'test', 1]) == 2
