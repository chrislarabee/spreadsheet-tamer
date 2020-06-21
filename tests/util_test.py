import inspect
from collections import OrderedDict as od
import string

import pytest
import pandas as pd
import numpy as np
from numpy import nan

import datagenius.util as u


def test_transmutation():
    @u.transmutation
    def func(x):
        return x
    assert func.stage == '_no_stage'
    assert func.args == ['x']

    @u.transmutation('rejects', stage='preprocess')
    def func(x):
        return x
    assert func.stage == 'preprocess'

    @u.transmutation(stage='a custom stage')
    def func(x):
        return x
    assert func.stage == 'a_custom_stage'


def test_align_args():
    assert u.align_args(
        lambda x, y: x + y, kwargs=dict(x=1, y=3)) == dict(x=1, y=3)
    assert u.align_args(
        lambda x, y: x + y, dict(x=1, y=3, z=2), 'y') == dict(x=1)
    assert u.align_args(
        lambda x, y: x + y, dict(x=1), ['y', 'z']) == dict(x=1)

    @u.transmutation
    def func(x, y, z):
        return x, y, z

    assert u.align_args(func, dict(x=1, y=2, z=3)) == dict(x=1, y=2, z=3)


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


def test_gen_empty_md_df():
    expected = pd.DataFrame([dict(a=0, b=0, c=0)])
    pd.testing.assert_frame_equal(
        u.gen_empty_md_df(['a', 'b', 'c']),
        expected
    )


def test_get_class_name():
    assert u.get_class_name('string') == 'str'
    assert u.get_class_name(123) == 'int'
    assert u.get_class_name(1.245) == 'float'
    assert u.get_class_name(nan) == 'nan'


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


def test_purge_gap_rows(gaps, gaps_totals):
    d = pd.DataFrame(gaps)
    d = u.purge_gap_rows(d)
    assert d.shape == (5, 4)
    d = pd.DataFrame(gaps_totals())
    d = u.purge_gap_rows(d)
    assert d.shape == (9, 3)


def test_standardize_header():
    header = pd.Index(
        ['Variant SKU', ' Barcode  2 ', 'Barcode  3']
    )
    assert u.standardize_header(header) == (
        ['variant_sku', 'barcode_2', 'barcode_3'],
        list(header)
    )


def test_translate_null():
    assert pd.isna(u.translate_null(None))
    assert pd.isna(u.translate_null(nan))
    assert u.translate_null(nan, None) is None
    assert u.translate_null(None, None) is None
    assert u.translate_null('string') == 'string'

    with pytest.raises(ValueError, match='must be numpy nan or None'):
        u.translate_null(1, int)


def test_tuplify():
    assert isinstance(u.tuplify('test'), tuple)
    assert u.tuplify('test') == ('test',)
    assert u.tuplify(None) is None
    assert u.tuplify(None, True) == (None,)


def test_validate_attr():
    df = pd.DataFrame([[1, 2, 3], [4, 5, 6]])
    assert u.validate_attr(df, 'shape', (2, 3))
    assert u.validate_attr(df, 'shape')
    assert not u.validate_attr(df, 'gibberish', 'nonsense')
