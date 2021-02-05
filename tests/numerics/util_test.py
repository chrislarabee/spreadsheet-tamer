from tamer.numerics import util as u
from tamer.numerics.zero_numeric import ZeroNumeric


class TestIsNumericPlus:
    def test_that_it_can_handle_an_integer(self):
        x = 1
        assert u.isnumericplus(x) == True
        assert u.isnumericplus(x, return_type=True) == (True, int)

    def test_that_it_can_handle_a_float(self):
        x = 1.5
        assert u.isnumericplus(x) == True
        assert u.isnumericplus(x, return_type=True) == (True, float)

    def test_that_it_can_handle_an_integer_stored_as_string(self):
        x = "1"
        assert u.isnumericplus(x) == True
        assert u.isnumericplus(x, return_type=True) == (True, int)

    def test_that_it_can_handle_a_float_stored_as_string(self):
        x = "1.5"
        assert u.isnumericplus(x) == True
        assert u.isnumericplus(x, return_type=True) == (True, float)

    def test_that_it_can_handle_a_zero_numeric(self):
        x = ZeroNumeric("000123")
        assert u.isnumericplus(x) == True
        assert u.isnumericplus(x, return_type=True) == (True, ZeroNumeric)

    def test_that_it_can_handle_a_zero_numeric_stored_as_string(self):
        x = "000123"
        assert u.isnumericplus(x) == True
        assert u.isnumericplus(x, return_type=True) == (True, ZeroNumeric)

    def test_that_it_can_handle_non_numeric_objects(self):
        x = "abc"
        assert u.isnumericplus(x) == False
        assert u.isnumericplus(x, return_type=True) == (False, str)
        x = [1, 2, 3]
        assert u.isnumericplus(x) == False
        assert u.isnumericplus(x, return_type=True) == (False, list)