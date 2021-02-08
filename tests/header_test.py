import pytest
import pandas as pd

from tamer.header import Header


class TestHeader:
    @pytest.fixture
    def labels(self):
        return ["A", "B", "C"]

    @pytest.fixture
    def sample_header(self, labels):
        return Header(labels)

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

    def test_equality_with_lists(self, labels):
        h = Header(labels)
        assert h == ["a", "b", "c"]
        assert h != ["a", "b", "d"]

    def test_eqality_with_other_objects(self, labels):
        h = Header(labels)
        assert not h == {"a", "b", "c"}
        assert not h == "abc"

    def test_that_values_can_be_set_and_immediately_standardized(self, sample_header):
        sample_header[0] = "Z"
        assert sample_header[0] == "z"
        assert sample_header.original_form[0] == "Z"
        sample_header[1] = "Z"
        assert sample_header[1] == "z_1"
        assert sample_header.original_form[1] == "Z"

    def test_that_it_can_be_assigned_to_a_pandas_dataframe_as_columns(self, sample_header):
        df = pd.DataFrame([[1, 2, 3], [4, 5, 6]])
        pd.testing.assert_index_equal(df.columns, pd.RangeIndex(0, 3, 1))
        df.columns = sample_header
        assert list(df.columns) == ["a", "b", "c"]

    class TestStandardize:
        def test_that_it_works_on_pandas_index(self):
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

        def test_that_it_works_on_pandas_range_index(self):
            header = pd.RangeIndex(0, 2, 1)
            assert Header._standardize(header) == ["0", "1"]

    class TestEnforceUniques:
        def test_that_it_works_on_integers(self):
            assert Header._enforce_uniques([1, 2, 3]) == [1, 2, 3]
            assert Header._enforce_uniques([1, 2, 2]) == [1, 2, "2_1"]

        def test_that_it_work_on_strings(self):
            assert Header._enforce_uniques(["x", "y"]) == ["x", "y"]
            assert Header._enforce_uniques(["x", "x", "y"]) == ["x", "x_1", "y"]

    class TestAppend:
        def test_that_values_can_be_appended(self, labels):
            h = Header(labels)
            h.append("Y$")
            assert h.original_form == [*labels, "Y$"]
            assert h == ["a", "b", "c", "y"]
            h.append("C")
            assert h == ["a", "b", "c", "y", "c_1"]

    class TestPop:
        def test_that_values_can_be_popped(self, sample_header):
            assert sample_header.pop(0) == "a"
            assert sample_header.original_form == ["B", "C"]

        def test_that_pop_raises_index_error_on_index_out_of_range(self, sample_header):
            with pytest.raises(IndexError, match="3 out of range"):
                sample_header.pop(3)

    class TestRemove:
        def test_that_it_can_remove_by_standardized_header(self, sample_header):
            sample_header.remove("c")
            assert sample_header == ["a", "b"]
            assert sample_header.original_form == ["A", "B"]
            sample_header.remove("B")
            assert sample_header == ["a"]
            assert sample_header.original_form == ["A"]

        def test_that_it_raises_value_error_on_missing_value(self, sample_header):
            with pytest.raises(ValueError, match="X not in Header."):
                sample_header.remove("X")
