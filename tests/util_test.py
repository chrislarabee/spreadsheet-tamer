from collections import OrderedDict as od

import datagenius.util as u
from datagenius.genius import parser


def test_collect_by_keys():
    x = u.collect_by_keys({'a': 1, 'b': 2, 'c': 3, 'd': 4}, 'a', 'c')
    assert x == {'a': 1, 'c': 3}
    assert isinstance(x, dict) and not isinstance(x, od)
    x = u.collect_by_keys(od(e=5, f=6, g=7), 'e', 'f')
    assert x == od(e=5, f=6)
    assert type(x) == od and type(x) != dict


def test_count_nulls():
    assert u.count_nulls(['', '', ''], strict=False) == 3
    assert u.count_nulls([1, '', '']) == 0
    assert u.count_nulls([1, 2, 3]) == 0
    assert u.count_nulls(od(x=1, y=None, z=''), strict=False) == 2
    assert u.count_nulls(dict(a='t', b='u', c='')) == 0


def test_count_true_str():
    assert u.count_true_str(['', '', '']) == 0
    assert u.count_true_str(['a', 'test', 1]) == 2


def test_validate_parser():
    assert not u.validate_parser('string')
    assert u.validate_parser(parser(lambda x: x + 1))
    assert not u.validate_parser(lambda x: x + 1)