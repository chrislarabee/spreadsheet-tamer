import pytest
import pandas as pd

from tamer.header import Header


class TestHeader:
    @pytest.fixture
    def labels(self):
        return ["A", "B", "C"]

    def test_that_standardize_works_on_pandas_index(self):
        header = pd.Index(
            [
                "Variant SKU",
                " Barcode  2 ",
                "Barcode  #3",
                "Barcode 3",
                "$ cost",
            ]
        )
        expected = [
            "variant_sku",
            "barcode_2",
            "barcode_3",
            "barcode_3_1",
            "cost",
        ]
        assert Header._standardize(header) == expected

    def test_that_standardize_works_on_pandas_range_index(self):
        header = pd.RangeIndex(0, 2, 1)
        assert Header._standardize(header) == ["0", "1"]

    def test_that_enforce_uniques_works_on_integers(self):
        assert Header._enforce_uniques([1, 2, 3]) == [1, 2, 3]
        assert Header._enforce_uniques([1, 2, 2]) == [1, 2, "2_1"]

    def test_that_enforce_uniques_work_on_strings(self):
        assert Header._enforce_uniques(["x", "y"]) == ["x", "y"]
        assert Header._enforce_uniques(["x", "x", "y"]) == ["x", "x_1", "y"]

    def test_that_it_can_handle_pandas_index_input(self):
        h = Header(pd.Index(["a", "b", "c"]))
        assert h == ['a', 'b', 'c']

    def test_that_it_can_handle_args_input(self):
        h = Header("a", "b", "c")
        assert h == ['a', 'b', 'c']

    def test_that_it_can_handle_list_input(self):
        expected = ['a', 'b', 'c']
        h = Header(*["a", "b", "c"])
        assert h == expected
        h = Header(["a", "b", "c"])
        assert h == expected

    def test_that_it_has_list_like_behavior(self, labels):
        h = Header(labels)
        assert h[0] == "a"
        assert h.original_form == labels
        assert len(h) == 3
        assert [i for i in h] == ["a", "b", "c"]

    def test_that_values_can_be_set_and_immediately_standardized(self, labels):
        h = Header(labels)
        h[0] = "Z"
        assert h[0] == "z"
        assert h.original_form[0] == "Z"
        h[1] = "Z"
        assert h[1] == "z_1"
        assert h.original_form[1] == "Z"

    def test_that_values_can_be_appended(self, labels):
        h = Header(labels)
        h.append("Y$")
        assert h.original_form == [*labels, "Y$"]
        assert h == ["a", "b", "c", "y"]
        h.append("C")
        assert h == ["a", "b", "c", "y", "c_1"]

    def test_that_values_can_be_deleted(self, labels):
        h = Header(labels)
        assert h.pop(0) == "a"
        assert h.original_form == ["B", "C"]
        h.remove("c")
        assert h == ["b"]
        assert h.original_form == ["B"]
        h.remove("B")
        assert h == []
        assert h.original_form == []

    def test_that_remove_raises_value_error_on_missing_value(self, labels):
        h = Header(labels)
        with pytest.raises(ValueError, match="X not in Header."):
            h.remove("X")

    def test_that_pop_raises_index_error_on_index_out_of_range(self, labels):
        h = Header(labels)
        with pytest.raises(IndexError, match="3 out of range"):
            h.pop(3)

    def test_equality_with_lists(self, labels):
        h = Header(labels)
        assert h == ["a", "b", "c"]
        assert h != ["a", "b", "d"]

    def test_eqality_with_other_objects(self, labels):
        h = Header(labels)
        assert not h == {"a", "b", "c"}
        assert not h == "abc"