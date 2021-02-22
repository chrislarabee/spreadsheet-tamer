import pytest
import pandas as pd
from numpy import nan

from tamer import guide as gd
from tamer.numerics.zero_numeric import ZeroNumeric


class TestRule:
    def test_that_targets_are_tuplified(self):
        r = gd.Rule("test", target="test")
        assert r._target == ("test",)
        r = gd.Rule("test", target=[1, 2, 3])
        assert r._target == (1, 2, 3)

    class TestValidateTargetIntoPatterns:
        def test_that_it_works_with_correct_input(self):
            gd.Rule._validate_target_into_patterns(
                (r"(\d{2})(\d{2})", r"(a)(b)"), "{0}-{1}"
            )

        def test_that_it_raises_the_expected_error_with_incorrect_input(self):
            with pytest.raises(ValueError, match=r"\(a\) does not match \{0\}-\{1\}"):
                gd.Rule._validate_target_into_patterns(
                    (r"(\d{2})(\d{2})", r"(a)"), "{0}-{1}"
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
                "size", target=[r"^sm?$", r"all"], into="small", is_pattern="target"
            )
            s = pd.Series(["sm", "s", "sall", nan])
            expected = pd.Series(["small", "small", "small", nan])
            s = r.map_transform_values(s)
            pd.testing.assert_series_equal(s, expected)

        def test_that_it_works_with_target_pattern_and_into_pattern(self):
            r = gd.Rule(
                "size", target=[r"^(\d{2})(\d{2})$"], into="{0}-{1}", is_pattern="into"
            )
            s = pd.Series([1214, "1618", 12345, nan])
            expected = pd.Series(["12-14", "16-18", 12345, nan])
            s = r.map_transform_values(s)
            pd.testing.assert_series_equal(s, expected)

    class TestCastValues:
        def test_that_it_works_with_simple_value_matching(self):
            r = gd.Rule("size", target="100", cast=float)
            s = pd.Series([1, "12", "100", nan])
            expected = pd.Series([1, "12", 100.0, nan])
            s = r.cast_values(s)
            pd.testing.assert_series_equal(s, expected)

        def test_that_it_works_with_target_pattern(self):
            r = gd.Rule("size", target=r"\d+$", cast=int, is_pattern="target")
            s = pd.Series([1, "12", "100", "test", nan])
            expected = pd.Series([1, 12, 100, "test", nan])
            s = r.cast_values(s)
            pd.testing.assert_series_equal(s, expected)

    class TestMarkRedistributionValues:
        def test_that_it_works_with_simple_value_matching(self):
            r = gd.Rule("size", target=["blue", "red", "green"], redistribute="color")
            s = pd.Series([nan, "XL", "blue", "green", "LG"])
            expected = pd.Series([nan, nan, "blue", "green", nan])
            s = r.get_redistribution_values(s)
            pd.testing.assert_series_equal(s, expected)

        def test_that_it_works_with_pattern_matching(self):
            r = gd.Rule(
                "size",
                target=[r"blue", r"light"],
                redistribute="color",
                is_pattern="target",
            )
            s = pd.Series([nan, "light brown", "dark blue", "cyan blue", "XL"])
            expected = pd.Series([nan, "light brown", "dark blue", "cyan blue", nan])
            s = r.get_redistribution_values(s)
            pd.testing.assert_series_equal(s, expected)


class TestGuide:
    class TestResolveRules:
        def test_that_it_works_for_type_conversion(self, customers):
            df = pd.DataFrame(**customers())
            g = gd.Guide(
                id=[gd.Rule("id", cast=int)],
                foreign_key=[gd.Rule("foreign_key", ZeroNumeric)],
            )
            df = g.resolve_rules(df)
            pd.testing.assert_frame_equal(df, pd.DataFrame(**customers(int)))
            assert list(df.dtypes) == ["int64", "O", "O", "O"]

        @pytest.fixture
        def redis_df(self):
            return pd.DataFrame(
                [
                    dict(a="red", b=nan),
                    dict(a="L", b="blue"),
                    dict(a="S", b=nan),
                    dict(a="yellow", b=1),
                    dict(a=123, b="x"),
                ]
            )

        def test_that_it_works_for_fillna_value_redistribution(self, redis_df):
            expected = pd.DataFrame(
                [
                    dict(a=nan, b="red"),
                    dict(a="L", b="blue"),
                    dict(a="S", b=nan),
                    dict(a=nan, b=1),
                    dict(a=123, b="x"),
                ]
            )
            g = gd.Guide(a=[gd.Rule("a", target=["red", "yellow"], redistribute="b")])
            df = g.resolve_rules(redis_df)
            pd.testing.assert_frame_equal(df, expected)

        def test_that_it_works_for_overwrite_value_redistribution(self, redis_df):
            expected = pd.DataFrame(
                [
                    dict(a=nan, b="red"),
                    dict(a="L", b="blue"),
                    dict(a="S", b=nan),
                    dict(a=nan, b="yellow"),
                    dict(a=nan, b=123),
                ]
            )
            g = gd.Guide(
                a=[
                    gd.Rule(
                        "a",
                        target=["red", "yellow", 123],
                        redistribute="b",
                        redistribute_mode="overwrite",
                    )
                ]
            )
            df = g.resolve_rules(redis_df)
            pd.testing.assert_frame_equal(df, expected)

        def test_that_it_works_for_append_value_redistribution(self, redis_df):
            expected = pd.DataFrame(
                [
                    dict(a=nan, b="red"),
                    dict(a="L", b="blue"),
                    dict(a="S", b=nan),
                    dict(a=nan, b="1 yellow"),
                    dict(a=nan, b="x 123"),
                ]
            )
            g = gd.Guide(
                a=[
                    gd.Rule(
                        "a",
                        target=["red", "yellow", 123],
                        redistribute="b",
                        redistribute_mode="append",
                    )
                ]
            )
            df = g.resolve_rules(redis_df)
            pd.testing.assert_frame_equal(df, expected)
