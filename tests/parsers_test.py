from datagenius import dataset as d
from datagenius import parsers as pa


def test_parser():
    # Decorator without arguments:
    @pa.parser
    def f(x):
        return x * 10

    assert f.is_parser
    assert not f.breaks_loop
    assert f.null_val is None

    # Decorator with arguments:
    @pa.parser(breaks_loop=True)
    def g(x):
        return x + 1

    assert g.breaks_loop
    assert g.null_val is None

    # Sanity check to ensure pre-built parsers work:
    assert not pa.cleanse_gap.breaks_loop

    # Sanity check to ensure lambda function parsers work:
    p = pa.parser(lambda x: x + 1, null_val=0)

    assert p.null_val == 0
    assert p(3) == 4


def test_cleanse_gap():
    assert pa.cleanse_gap([1, 2, 3]) == [1, 2, 3]
    assert pa.cleanse_gap(['', '', '']) is None
    assert pa.cleanse_gap(['', '', ''], 0) == ['', '', '']
    assert pa.cleanse_gap([1, 2, None], 3) is None
    assert pa.cleanse_gap([1, 2, None], 2) == [1, 2, None]


def test_detect_header():
    assert pa.detect_header([1, 2, 3]) is None
    assert pa.detect_header(['a', 'b', 'c']) == ['a', 'b', 'c']
