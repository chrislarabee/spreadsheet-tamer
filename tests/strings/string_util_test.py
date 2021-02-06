from tamer.strings import string_util as u


class TestCleanWhitespace:
    def test_that_it_can_handle_non_strings(self):
        assert u.clean_whitespace(1) == (False, 1)

    def test_that_it_flags_untouched_strings(self):
        assert u.clean_whitespace("a good string") == (False, "a good string")

    def test_that_it_can_clean_strings_with_bizarre_spacing(self):
        assert u.clean_whitespace(" a bad  string ") == (True, "a bad string")
        assert u.clean_whitespace("     what       even     ") == (True, "what even")
        