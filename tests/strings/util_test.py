import pandas as pd
from numpy import nan

from tamer.strings import util as u


class TestCleanWhitespace:
    def test_that_it_can_handle_non_strings(self):
        assert u.clean_whitespace(1) == (False, 1)

    def test_that_it_flags_untouched_strings(self):
        assert u.clean_whitespace("a good string") == (False, "a good string")

    def test_that_it_can_clean_strings_with_bizarre_spacing(self):
        assert u.clean_whitespace(" a bad  string ") == (True, "a bad string")
        assert u.clean_whitespace("     what       even     ") == (True, "what even")


class TestCountTrueStr:
    def test_that_it_ignores_blank_strings(self):
        assert u.count_true_str(["", "", ""]) == 0

    def test_that_it_can_handle_sequences_of_mixed_types(self):
        assert u.count_true_str(["a", "test", 1]) == 2

    def test_that_it_can_handle_pandas_series(self):
        assert u.count_true_str(pd.Series(["a", "test", 1])) == 2
        assert u.count_true_str(pd.Series([nan, "test", 1])) == 1
