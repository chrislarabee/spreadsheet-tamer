import pytest

from datagenius.dataset import Dataset
from datagenius import parsers as pa


class TestDataset:
    def test_getitem(self):
        d = Dataset([
            [1, 2, 3],
            [4, 5, 6]
        ])

        assert d[0] == [1, 2, 3]

    def test_index(self):
        d = Dataset([
            [1, 2, 3],
            [4, 5, 6]
        ])

        assert d.index([1, 2, 3]) == 0
