from datagenius.dataset import Dataset
from datagenius.genius import Genius


class TestGenius:
    def test_header_func(self, simple_data, gaps):
        expected = ['id', 'fname', 'lname']
        assert Genius.header_func(Dataset(simple_data)) == (
            0, expected)
        assert Genius.header_func(Dataset(gaps)) == (
            4, expected)
