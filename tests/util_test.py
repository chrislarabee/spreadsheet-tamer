from collections import OrderedDict as od
import string

import pandas as pd
import numpy as np

import datagenius.util as u
from datagenius.genius import parser


def test_clean_whitespace():
    assert u.clean_whitespace(1) == [False, 1]
    assert u.clean_whitespace(' a bad  string ') == [True, 'a bad string']
    assert u.clean_whitespace('a good string') == [False, 'a good string']
    assert u.clean_whitespace(
        '     what       even     ') == [True, 'what even']


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
    assert u.count_true_str(pd.Series(['a', 'test', 1])) == 2
    assert u.count_true_str(pd.Series([np.nan, 'test', 1])) == 1


def test_gen_alpha_keys():
    assert u.gen_alpha_keys(5) == {'A', 'B', 'C', 'D', 'E'}
    assert u.gen_alpha_keys(26) == set(string.ascii_uppercase)
    assert u.gen_alpha_keys(27) == {*string.ascii_uppercase, 'AA'}
    assert u.gen_alpha_keys(28) == {*string.ascii_uppercase, 'AA', 'AB'}
    assert u.gen_alpha_keys(53) == {
        *string.ascii_uppercase, *['A' + a for a in string.ascii_uppercase],
        'BA'}


def test_isnumericplus():
    assert u.isnumericplus(1)
    assert u.isnumericplus(2.25)
    assert u.isnumericplus('1234')
    assert u.isnumericplus('1234.56')
    assert u.isnumericplus('1234..56')
    assert u.isnumericplus('1234.56789')
    assert not u.isnumericplus('100 strings')
    assert u.isnumericplus(1, '-v') == (True, int)
    assert u.isnumericplus(1.0, '-v') == (True, float)
    assert u.isnumericplus(2.25, '-v') == (True, float)
    assert u.isnumericplus('100 strings', '-v') == (False, str)
    assert u.isnumericplus('1234', '-convert') == (True, 1234)
    assert u.isnumericplus('1234..56', '-convert') == (True, 1234.56)
    assert u.isnumericplus('100 strings', '-convert') == (False, '100 strings')
    assert u.isnumericplus('1234.56', '-v', '-convert') == (
        True, float, 1234.56)


def test_translate_nans():
    d = [
        [np.nan, 1, 2],
        [3, 4, np.nan]
    ]
    expected = [
        [None, 1, 2],
        [3, 4, None]
    ]
    assert u.translate_nans(d) == expected

    d = [
        od(a=np.nan, b=1, c=2),
        od(a=3, b=4, c=np.nan)
    ]
    expected = [
        od(a=None, b=1, c=2),
        od(a=3, b=4, c=None)
    ]
    assert u.translate_nans(d) == expected


def test_tuplify():
    assert isinstance(u.tuplify('test'), tuple)
    assert u.tuplify('test') == ('test',)
    assert u.tuplify(None) is None
    assert u.tuplify(None, True) == (None,)


def test_validate_parser():
    assert not u.validate_parser('string')
    assert u.validate_parser(parser(lambda x: x + 1))
    assert not u.validate_parser(lambda x: x + 1)