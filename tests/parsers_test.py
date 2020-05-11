from datagenius import dataset as d
from datagenius import parsers as pa


def test_parser():
    @pa.parser
    def f(x):
        return x * 10

    assert not f.breaks_loop
    assert f.null_val is None

    @pa.parser(breaks_loop=True)
    def g(x):
        return x + 1

    assert g.breaks_loop
    assert g.null_val is None

    assert not pa.non_null_count.breaks_loop


def test_cleanse_gaps(simple_data, gaps):
    assert pa.cleanse_gaps(d.Dataset(gaps)) == simple_data()


def test_non_null_count():
    assert pa.non_null_count(['', '', '']) == 0
    assert pa.non_null_count([1, '', '']) == 1
    assert pa.non_null_count([1, 2, 3]) == 3
