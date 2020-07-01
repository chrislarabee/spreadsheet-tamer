from collections import OrderedDict as od
import string

import pytest
import pandas as pd
import numpy as np
from numpy import nan

import datagenius.util as u
import datagenius.element as e


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


def test_nullable():
    @u.nullable
    def func(x):
        return x[0]
    assert func([1, 2, 3]) == 1
    assert pd.isna(func(nan))


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


def test_broadcast_suffix():
    assert u.broadcast_suffix(
        ['x', 'y', 'z'], '_1') == ['x_1', 'y_1', 'z_1']
    assert u.broadcast_suffix(
        pd.Index(['x', 'y', 'z']), '_1') == ['x_1', 'y_1', 'z_1']
    assert u.broadcast_suffix(
        pd.Series(['x', 'y', 'z']), '_1') == ['x_1', 'y_1', 'z_1']


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


def test_enforce_uniques():
    assert u.enforce_uniques([1, 2, 3]) == [1, 2, 3]
    assert u.enforce_uniques(['x', 'x', 'y']) == ['x', 'x_1', 'y']
    assert u.enforce_uniques([1, 2, 2]) == [1, 2, '2_1']


def test_gen_alpha_keys():
    assert u.gen_alpha_keys(5) == ['A', 'B', 'C', 'D', 'E']
    assert u.gen_alpha_keys(26) == list(string.ascii_uppercase)
    assert u.gen_alpha_keys(27) == [*string.ascii_uppercase, 'AA']
    assert u.gen_alpha_keys(28) == [*string.ascii_uppercase, 'AA', 'AB']
    assert u.gen_alpha_keys(53) == [
        *string.ascii_uppercase, *['A' + a for a in string.ascii_uppercase],
        'BA']


def test_gen_empty_md_df():
    expected = pd.DataFrame([dict(a=0, b=0, c=0)])
    pd.testing.assert_frame_equal(
        u.gen_empty_md_df(['a', 'b', 'c']),
        expected
    )

    expected = pd.DataFrame([dict(a='x', b='x', c='x')])
    pd.testing.assert_frame_equal(
        u.gen_empty_md_df(['a', 'b', 'c'], 'x'),
        expected
    )


def test_get_class_name():
    assert u.get_class_name('string') == 'str'
    assert u.get_class_name(123) == 'int'
    assert u.get_class_name(1.245) == 'float'
    assert u.get_class_name(nan) == 'nan'


def test_gconvert():
    assert u.gconvert(123, str) == '123'
    assert u.gconvert('1..23', float) == 1.23
    assert pd.isna(u.gconvert(nan, int))
    with pytest.raises(
            ValueError, match='target_type must be one of'):
        u.gconvert(123, dict)


def test_gtype():
    assert u.gtype(1) == int
    assert u.gtype('test') == str
    assert u.gtype(2.1) == float
    assert pd.isna(u.gtype(nan))


def test_gwithin():
    assert u.gwithin([1, 2, 3], 1)
    assert u.gwithin([1, 2, 3], 1, 4)
    assert not u.gwithin([1, 2, 3], 4, 5)
    assert u.gwithin(['xyz', 'a23'], r'[a-z]\d+')
    assert not u.gwithin(['xyz', 'a23'], r'[a-z]\d[a-z]')
    assert u.gwithin(
        pd.Index(['unnamed_0', 'unnamed_1']), r'[Uu]nnamed:*[ _]\d')
    assert u.gwithin(
        pd.Index(['Unnamed: 0', 'Unnamed: 1']), r'[Uu]nnamed:*[ _]\d')
    assert u.gwithin(
        pd.Index(['Unnamed:_0', 'Unnamed:_1']), r'[Uu]nnamed:*[ _]\d')


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
    assert u.isnumericplus('00123')
    assert u.isnumericplus('00123', '-v') == (True, e.ZeroNumeric)
    assert u.isnumericplus('123', '-no_bool', '-convert') == 123
    assert u.isnumericplus('0.00', '-v') == (True, float)


def test_purge_gap_rows(gaps, gaps_totals):
    d = pd.DataFrame(gaps)
    d = u.purge_gap_rows(d)
    assert d.shape == (5, 4)
    d = pd.DataFrame(gaps_totals())
    d = u.purge_gap_rows(d)
    assert d.shape == (9, 3)


def test_standardize_header():
    header = pd.Index(
        ['Variant SKU', ' Barcode  2 ', 'Barcode  #3', 'Barcode 3']
    )
    assert u.standardize_header(header) == (
        ['variant_sku', 'barcode_2', 'barcode_3', 'barcode_3_1'],
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


def test_tuplify_iterable():
    assert u.tuplify_iterable([1, 2, 3]) == [(1,), (2,), (3,)]


def test_validate_attr():
    df = pd.DataFrame([[1, 2, 3], [4, 5, 6]])
    assert u.validate_attr(df, 'shape', (2, 3))
    assert u.validate_attr(df, 'shape')
    assert not u.validate_attr(df, 'gibberish', 'nonsense')
