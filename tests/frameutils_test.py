import pytest
import pandas as pd
from numpy import nan

from tamer import frameutils
from tamer.decorators import nullable


class TestComplexJoinRule:
    def test_that_its_subscriptable(self):
        r = frameutils.ComplexJoinRule("a", "b", "c", inexact=True)
        assert r.thresholds[0] == 0.9

    def test_that_output_works(self):
        r = frameutils.ComplexJoinRule("a", "b", "c", conditions=dict(c="x"))
        assert r.output() == (("a", "b", "c"), {"c": ("x",)})
        assert r.output("on", "thresholds") == (("a", "b", "c"), None)
        assert r.output("on") == ("a", "b", "c")


class TestComplexJoinDaemon:
    class TestDoExact:
        def test_that_it_works_as_expected(self, sales, regions):
            df1 = pd.DataFrame(**sales)
            df2 = pd.DataFrame(**regions)
            result = frameutils.ComplexJoinDaemon.do_exact(df1, df2, ("region",))
            assert list(result.stores) == [50, 50, 42, 42]
            assert list(result.employees) == [500, 500, 450, 450]

    class TestDoInexact:
        def test_that_it_can_do_exact_as_sanity_check(self, sales, regions):
            df1 = pd.DataFrame(**sales)
            df2 = pd.DataFrame(**regions)
            result = frameutils.ComplexJoinDaemon.do_inexact(
                df1, df2, ("region",), thresholds=(1,)
            )
            assert list(result.stores) == [50, 50, 42, 42]
            assert list(result.employees) == [500, 500, 450, 450]

        def test_that_it_works_with_a_basic_inexact_match(self, sales, stores):
            df1 = pd.DataFrame(**sales)
            df2 = pd.DataFrame(**stores)
            result = frameutils.ComplexJoinDaemon.do_inexact(
                df1, df2, ("location",), thresholds=(0.7,)
            )
            assert list(result.budget) == [100000, 90000, 110000, 90000]
            assert list(result.inventory) == [5000, 4500, 4500, 4500]
            assert (
                set(result.columns).difference(
                    {
                        "location",
                        "region",
                        "region_s",
                        "sales",
                        "location_s",
                        "budget",
                        "inventory",
                    }
                )
                == set()
            )

        def test_that_it_works_with_block(self, sales, stores):
            df1 = pd.DataFrame(**sales)
            df2 = pd.DataFrame(**stores)
            result = frameutils.ComplexJoinDaemon.do_inexact(
                df1, df2, ("location",), thresholds=(0.7,), block=("region",)
            )
            assert list(result.budget) == [100000, 90000, 110000, 90000]
            assert list(result.inventory) == [5000, 4500, 4500, 4500]
            assert (
                set(result.columns).difference(
                    {
                        "location",
                        "region",
                        "region_s",
                        "sales",
                        "location_s",
                        "budget",
                        "inventory",
                    }
                )
                == set()
            )

        def test_that_it_works_with_multiple_ons(self, sales, stores):
            df1 = pd.DataFrame(**sales)
            df2 = pd.DataFrame(**stores)
            result = frameutils.ComplexJoinDaemon.do_inexact(
                df1, df2, ("location", "region"), thresholds=(0.7, 1)
            )
            assert list(result.budget) == [100000, 90000, 110000, 90000]
            assert list(result.inventory) == [5000, 4500, 4500, 4500]
            assert (
                set(result.columns).difference(
                    {
                        "location",
                        "region",
                        "region_s",
                        "sales",
                        "location_s",
                        "budget",
                        "inventory",
                    }
                )
                == set()
            )

    class TestChunkDataFrames:
        def test_that_it_works_with_a_single_dataframe(self, stores):
            df = pd.DataFrame(**stores)
            plan = (
                frameutils.ComplexJoinRule("location", conditions=dict(budget=90000)),
                frameutils.ComplexJoinRule("budget", conditions=dict(inventory=4500)),
            )
            c, p_df = frameutils.ComplexJoinDaemon._chunk_dataframes(plan, df)
            assert c[0][0].values.tolist() == [
                ["W Valley", "Northern", 90000, 4500],
                ["Kalliope", "Southern", 90000, 4500],
            ]
            assert c[1][0].values.tolist() == [
                ["Precioso", "Southern", 110000, 4500],
            ]
            assert p_df.values.tolist() == [
                ["Bayside", "Northern", 100000, 5000],
            ]

        def test_that_it_works_with_multiple_dataframes(self, sales, regions):
            df1 = pd.DataFrame(**sales)
            df2 = pd.DataFrame(**regions)
            plan = (
                frameutils.ComplexJoinRule(
                    "region", conditions=dict(region=("Northern",))
                ),
            )
            c, p_df = frameutils.ComplexJoinDaemon._chunk_dataframes(plan, df1, df2)
            expected = pd.DataFrame(**sales).iloc[[0, 1]]
            pd.testing.assert_frame_equal(c[0][0], expected)
            assert c[0][1].to_dict("records") == [
                dict(region="Northern", stores=50, employees=500)
            ]
            expected = pd.DataFrame(**sales).iloc[[2, 3]]
            pd.testing.assert_frame_equal(p_df, expected)

        def test_that_it_works_with_multiple_dataframes_and_no_conditions(
            self, sales, regions
        ):
            df1 = pd.DataFrame(**sales)
            df2 = pd.DataFrame(**regions)
            plan = (frameutils.ComplexJoinRule("region"),)
            c, p_df = frameutils.ComplexJoinDaemon._chunk_dataframes(plan, df1, df2)
            expected = pd.DataFrame(**sales)
            pd.testing.assert_frame_equal(c[0][0], expected)
            expected = pd.DataFrame(**regions)
            pd.testing.assert_frame_equal(c[0][1], expected)
            assert p_df.to_dict("records") == []

    class TestSliceDataFrame:
        def test_that_it_works_with_a_single_condition(self, stores):
            df = pd.DataFrame(**stores)
            expected = pd.DataFrame(
                [
                    ["W Valley", "Northern", 90000, 4500],
                    ["Precioso", "Southern", 110000, 4500],
                    ["Kalliope", "Southern", 90000, 4500],
                ],
                columns=["location", "region", "budget", "inventory"],
                index=[1, 2, 3],
            )
            x = frameutils.ComplexJoinDaemon._slice_dataframe(
                df, {"inventory": (4500,)}
            )
            assert df is not x[0]
            assert x[1]
            pd.testing.assert_frame_equal(x[0], expected)

        def test_that_it_works_with_multiple_conditions(self, stores):
            df = pd.DataFrame(**stores)
            expected = pd.DataFrame(
                [
                    ["W Valley", "Northern", 90000, 4500],
                    ["Kalliope", "Southern", 90000, 4500],
                ],
                columns=["location", "region", "budget", "inventory"],
                index=[1, 3],
            )
            x = frameutils.ComplexJoinDaemon._slice_dataframe(
                df, {"inventory": (4500,), "budget": (90000,)}
            )
            assert df is not x[0]
            assert x[1]
            pd.testing.assert_frame_equal(x[0], expected)

        def test_that_it_works_with_no_conditions(self, stores):
            df = pd.DataFrame(**stores)
            expected = pd.DataFrame(**stores)
            x = frameutils.ComplexJoinDaemon._slice_dataframe(df, {None: (None,)})
            assert df is not x[0]
            assert x[1]
            pd.testing.assert_frame_equal(x[0], expected)

        def test_that_it_works_with_unmet_conditions(self, stores):
            df = pd.DataFrame(**stores)
            expected = pd.DataFrame(
                [],
                columns=["location", "region", "budget", "inventory"],
            )
            x = frameutils.ComplexJoinDaemon._slice_dataframe(df, {"budget": (25000,)})
            assert df is not x[0]
            assert not x[1]
            pd.testing.assert_frame_equal(
                x[0], expected, check_index_type=False, check_dtype=False
            )

    class TestBuildPlan:
        def test_that_it_works_with_simple_ons(self):
            plan = frameutils.ComplexJoinDaemon._build_plan(("a", "b", "c"))
            assert plan[0].output() == (("a", "b", "c"), {None: (None,)})

        def test_that_condition_on_pairs_can_be_in_any_order(self):
            expected_plan_0 = (("a",), {"c": ("x",)})
            expected_plan_1 = (("a", "b"), {None: (None,)})
            plan = frameutils.ComplexJoinDaemon._build_plan(
                ("a", "b", ("a", {"c": "x"}))
            )
            assert plan[0].output() == expected_plan_0
            assert plan[1].output() == expected_plan_1
            plan = frameutils.ComplexJoinDaemon._build_plan(
                ("a", "b", ({"c": "x"}, "a"))
            )
            assert plan[0].output() == expected_plan_0
            assert plan[1].output() == expected_plan_1
            plan = frameutils.ComplexJoinDaemon._build_plan((({"c": "x"}, "a"),))
            assert plan[0].output() == expected_plan_0
            assert len(plan) == 1

    class TestPrepOns:
        def test_that_it_wraps_tuples_and_strings_in_tuples(self):
            assert frameutils.ComplexJoinDaemon._prep_ons(("a", "b")) == (("a", "b"),)
            assert frameutils.ComplexJoinDaemon._prep_ons("a") == ("a",)

    class TestPrepSuffixes:
        def test_that_it_works_with_no_suffixes_supplied(self):
            assert frameutils.ComplexJoinDaemon._prep_suffixes(2) == ("_A", "_B")

        def test_that_it_works_with_suffixes_supplied(self):
            assert frameutils.ComplexJoinDaemon._prep_suffixes(2, ("_x", "_y")) == (
                "_x",
                "_y",
            )
            assert frameutils.ComplexJoinDaemon._prep_suffixes(1, "_x") == ("_x",)

        def test_that_it_raises_expected_error_with_improper_suffixes_length(self):
            with pytest.raises(ValueError, match="Suffix len=2, suffixes="):
                frameutils.ComplexJoinDaemon._prep_suffixes(3, ("_x", "_y"))


class TestAccrete:
    def test_that_it_works_with_a_single_group_by(self):
        df = pd.DataFrame(
            [
                dict(a="t", b="u", c="v", d=1),
                dict(a="t", b="w", c=nan, d=2),
                dict(a="y", b="z", c="z", d=3),
            ]
        )
        expected = pd.DataFrame(
            [
                dict(a="t", b="u,w", c="v", d=1),
                dict(a="t", b="u,w", c="v", d=2),
                dict(a="y", b="z", c="z", d=3),
            ]
        )
        df = frameutils.accrete(df, ["a"], ("b", "c"), ",")
        pd.testing.assert_frame_equal(df, expected)

    def test_that_it_works_with_multiple_group_by_values(self):
        df = pd.DataFrame(
            [
                dict(a="t", b="u", c=1),
                dict(a="t", b="u", c=2),
                dict(a="x", b="y", c=nan),
            ]
        )
        expected = pd.DataFrame(
            [
                dict(a="t", b="u", c="1.0 2.0"),
                dict(a="t", b="u", c="1.0 2.0"),
                dict(a="x", b="y", c=nan),
            ]
        )
        df = frameutils.accrete(df, ["a", "b"], "c")
        pd.testing.assert_frame_equal(df, expected)


class TestMultiapply:
    def test_that_it_works_with_broadcasting_a_simple_lambda_func(self):
        df = pd.DataFrame([dict(a=1, b=2, c=3), dict(a=4, b=5, c=6)])
        expected = pd.DataFrame([dict(a=2, b=2, c=6), dict(a=8, b=5, c=12)])
        df = frameutils.multiapply(df, "a", "c", func=lambda x: x * 2)
        pd.testing.assert_frame_equal(df, expected)

    @pytest.fixture
    def sample_df(self) -> pd.DataFrame:
        return pd.DataFrame(
            [
                [1, "abc", 8.5, "foo"],
                [2, "def", 4.0, "bar"],
                [3, "ghi", 9.50, "spam"],
                [4, "jkl", 5.2, "eggs"],
            ],
            columns=["a", "b", "c", "d"],
        )

    @pytest.fixture
    def expected_df(self) -> pd.DataFrame:
        return pd.DataFrame(
            [
                [1, "ABC", 8.5, "Foo"],
                [2, "DEF", 4.0, "Bar"],
                [3, "GHI", 9.50, "Spam"],
                [4, "JKL", 5.2, "Eggs"],
            ],
            columns=["a", "b", "c", "d"],
        )

    def test_that_it_works_with_column_func_pairs(self, sample_df, expected_df):
        df = frameutils.multiapply(sample_df, b=str.upper, d=str.title)
        pd.testing.assert_frame_equal(df, expected_df)

    def test_that_it_works_with_nullable_funcs(self):
        df = pd.DataFrame(
            [
                [1, "abc", 8.5, "foo"],
                [2, nan, 4.0, "bar"],
                [3, "def", 9.50, "spam"],
                [4, "ghi", 5.2, nan],
            ],
            columns=["a", "b", "c", "d"],
        )
        expected = pd.DataFrame(
            [
                [1, "Abc", 8.5, "Foo"],
                [2, nan, 4.0, "Bar"],
                [3, "Def", 9.50, "Spam"],
                [4, "Ghi", 5.2, nan],
            ],
            columns=["a", "b", "c", "d"],
        )

        @nullable
        def nullable_title(x):
            return x.title()

        df = frameutils.multiapply(df, "b", "d", func=nullable_title)
        pd.testing.assert_frame_equal(df, expected)

    def test_that_it_works_with_a_mix_of_pairs_and_broadcasting(self, sample_df):
        expected = pd.DataFrame(
            [
                [1, "ABC", 17.0, "FOO"],
                [2, "DEF", 8.0, "BAR"],
                [3, "GHI", 19.0, "SPAM"],
                [4, "JKL", 10.4, "EGGS"],
            ],
            columns=["a", "b", "c", "d"],
        )
        df = frameutils.multiapply(
            sample_df, "b", "d", func=str.upper, c=lambda x: x * 2
        )
        pd.testing.assert_frame_equal(df, expected)
