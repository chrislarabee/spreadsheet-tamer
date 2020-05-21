from collections import OrderedDict as od

import datagenius.util as u
from datagenius.genius import parser


def test_non_null_count():
    assert u.non_null_count(['', '', '']) == 0
    assert u.non_null_count([1, '', '']) == 1
    assert u.non_null_count([1, 2, 3]) == 3
    assert u.non_null_count(od(x=1, y=None, z='')) == 1
    assert u.non_null_count(dict(a='t', b='u', c='')) == 2


def test_true_str_count():
    assert u.true_str_count(['', '', '']) == 0
    assert u.true_str_count(['a', 'test', 1]) == 2


def test_validate_parser():
    assert not u.validate_parser('string')
    assert u.validate_parser(parser(lambda x: x + 1))
    assert not u.validate_parser(lambda x: x + 1)