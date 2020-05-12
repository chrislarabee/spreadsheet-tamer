from datagenius import dataset as d
from datagenius import parsers as pa


def test_parser():
    # Decorator without arguments:
    @pa.parser
    def f(x):
        return x * 10

    assert not f.breaks_loop
    assert f.null_val is None

    # Decorator with arguments:
    @pa.parser(breaks_loop=True)
    def g(x):
        return x + 1

    assert g.breaks_loop
    assert g.null_val is None

    # Sanity check to ensure pre-built parsers work:
    assert not pa.cleanse_gaps.breaks_loop

    # Sanity check to ensure lambda function parsers work:
    p = pa.parser(lambda x: x + 1, null_val=0)

    assert p.null_val == 0
    assert p(3) == 4


def test_cleanse_gaps(simple_data, gaps):
    assert pa.cleanse_gaps(d.Dataset(gaps)) == simple_data()
