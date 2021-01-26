import datagenius.names.util as u


class TestLoadPatterns:
    def test_load_w_out_custom(self):
        result = u.load_patterns()
        assert isinstance(result, dict)
        