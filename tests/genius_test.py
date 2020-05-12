from datagenius.dataset import Dataset
import datagenius.genius as ge


class TestPreprocess:
    def test_go(self, simple_data, gaps, gaps_totals):
        p = ge.Preprocess()
        d = Dataset(simple_data())
        assert p.go(d) == simple_data()
        assert p.go(d) == d
        assert d.header == ['id', 'fname', 'lname', 'foreign_key']

        d = Dataset(gaps)
        assert p.go(d) == simple_data()
