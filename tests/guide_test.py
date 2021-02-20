import pytest
import pandas as pd
from numpy import nan

from tamer import guide as gd


class TestRule:
    class TestValidateTargetIntoPatterns:
        def test_that_it_works_with_correct_input(self):
            gd.Rule._validate_target_into_patterns(
                [r"(\d{2})(\d{2})", r"(a)(b)"], "{0}-{1}"
            )

        def test_that_it_raises_the_expected_error_with_incorrect_input(self):
            with pytest.raises(
                ValueError, match=r"\(a\) does not match \{0\}-\{1\}"
            ):
                gd.Rule._validate_target_into_patterns(
                    [r"(\d{2})(\d{2})", r"(a)"], "{0}-{1}"
                )

    class TestCountChar:
        def test_that_it_works_with_basic_chars(self):
            assert gd.Rule._count_char("baba", "a") == 2
            assert gd.Rule._count_char("baba", "o") == 0

        def test_that_it_works_with_special_chars(self):
            assert gd.Rule._count_char("(123)(456)", r"\(") == 2
            assert gd.Rule._count_char("(123)(456)", r"\{") == 0

    class TestMapTransformValues:
        def test_that_it_works_with_simple_value_mapping(self):
            r = gd.Rule("size", target=["sm", "s"], into="small")
            s = pd.Series(["sm", "s", nan])
            expected = pd.Series(["small", "small", nan])
            s = r.map_transform_values(s)
            pd.testing.assert_series_equal(s, expected)

        def test_that_it_works_with_target_pattern(self):
            r = gd.Rule(
                "size", target=[r"^sm?$", r"all"], into="small", target_pattern=True
            )
            s = pd.Series(["sm", "s", "sall", nan])
            expected = pd.Series(["small", "small", "small", nan])
            s = r.map_transform_values(s)
            pd.testing.assert_series_equal(s, expected)

        def test_that_it_works_with_target_pattern_and_into_pattern(self):
            r = gd.Rule(
                "size",
                target=[r"^(\d{2})(\d{2})$"],
                into="{0}-{1}",
                target_pattern=True,
                into_pattern=True,
            )
            s = pd.Series([1214, "1618", 12345, nan])
            expected = pd.Series(["12-14", "16-18", 12345, nan])
            s = r.map_transform_values(s)
            pd.testing.assert_series_equal(s, expected)
