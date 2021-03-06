import pandas as pd
import pytest

import datagenius.lib.supplement as su
import datagenius.lib.guides as gd


def test_do_exact(sales, regions):
    df1 = pd.DataFrame(**sales)
    df2 = pd.DataFrame(**regions)
    result = su.do_exact(df1, df2, ("region",))
    assert list(result.stores) == [50, 50, 42, 42]
    assert list(result.employees) == [500, 500, 450, 450]


def test_do_inexact(sales, regions, stores):
    # Make sure inexact can replicate exact, just as a sanity
    # check:
    df1 = pd.DataFrame(**sales)
    df2 = pd.DataFrame(**regions)
    result = su.do_inexact(df1, df2, ("region",), thresholds=(1,))
    assert list(result.stores) == [50, 50, 42, 42]
    assert list(result.employees) == [500, 500, 450, 450]

    # Now for a real inexact match:
    df3 = pd.DataFrame(**stores)
    result = su.do_inexact(df1, df3, ("location",), thresholds=(0.7,))
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

    # Same match, but with block:
    df3 = pd.DataFrame(**stores)
    result = su.do_inexact(
        df1, df3, ("location",), thresholds=(0.7,), block=("region",)
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

    # Same match, but with multiple ons:
    df3 = pd.DataFrame(**stores)
    result = su.do_inexact(df1, df3, ("location", "region"), thresholds=(0.7, 1))
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


def test_chunk_dframes(stores, sales, regions):
    df = pd.DataFrame(**stores)
    plan = su.build_plan(
        (({"budget": (90000,)}, "location"), ({"inventory": (4500,)}, "budget"))
    )
    c, p_df = su.chunk_dframes(plan, df)
    assert c[0][0].to_dict("records") == [
        dict(location="W Valley", region="Northern", budget=90000, inventory=4500),
        dict(location="Kalliope", region="Southern", budget=90000, inventory=4500),
    ]
    assert c[1][0].to_dict("records") == [
        dict(location="Precioso", region="Southern", budget=110000, inventory=4500)
    ]
    assert p_df.to_dict("records") == [
        dict(location="Bayside", region="Northern", budget=100000, inventory=5000)
    ]
    # Test multiple dframes:
    df1 = pd.DataFrame(**sales)
    df2 = pd.DataFrame(**regions)
    # Test with no conditions:
    plan = su.build_plan((({None: (None,)}, "region"),))
    c, p_df = su.chunk_dframes(plan, df1, df2)
    assert c[0][0].to_dict("records") == [
        dict(location="Bayside Store", region="Northern", sales=500),
        dict(location="West Valley Store", region="Northern", sales=300),
        dict(location="Precioso Store", region="Southern", sales=1000),
        dict(location="Kalliope Store", region="Southern", sales=200),
    ]
    assert c[0][1].to_dict("records") == [
        dict(region="Northern", stores=50, employees=500),
        dict(region="Southern", stores=42, employees=450),
    ]
    assert p_df.to_dict("records") == []
    # Test with conditions
    df1 = pd.DataFrame(**sales)
    df2 = pd.DataFrame(**regions)
    plan = su.build_plan((({"region": ("Northern",)}, "region"),))
    c, p_df = su.chunk_dframes(plan, df1, df2)
    assert c[0][0].to_dict("records") == [
        dict(location="Bayside Store", region="Northern", sales=500),
        dict(location="West Valley Store", region="Northern", sales=300),
    ]
    assert c[0][1].to_dict("records") == [
        dict(region="Northern", stores=50, employees=500)
    ]
    assert p_df.to_dict("records") == [
        dict(location="Precioso Store", region="Southern", sales=1000),
        dict(location="Kalliope Store", region="Southern", sales=200),
    ]


def test_slice_dframe(stores):
    df = pd.DataFrame(**stores)
    expected = [
        dict(location="W Valley", region="Northern", budget=90000, inventory=4500),
        dict(location="Precioso", region="Southern", budget=110000, inventory=4500),
        dict(location="Kalliope", region="Southern", budget=90000, inventory=4500),
    ]
    x = su.slice_dframe(df, {"inventory": (4500,)})
    assert df is not x[0]
    assert x[0].to_dict("records") == expected
    assert x[1]

    # Test multiple conditions:
    expected = [
        dict(location="W Valley", region="Northern", budget=90000, inventory=4500),
        dict(location="Kalliope", region="Southern", budget=90000, inventory=4500),
    ]
    x = su.slice_dframe(df, {"inventory": (4500,), "budget": (90000,)})
    assert x[0].to_dict("records") == expected
    assert x[1]

    # Test no conditions:
    expected = [
        dict(location="Bayside", region="Northern", budget=100000, inventory=5000),
        dict(location="W Valley", region="Northern", budget=90000, inventory=4500),
        dict(location="Precioso", region="Southern", budget=110000, inventory=4500),
        dict(location="Kalliope", region="Southern", budget=90000, inventory=4500),
    ]
    x = su.slice_dframe(df, {None: (None,)})
    assert x[0].to_dict("records") == expected
    assert x[1]

    # Test unmet conditions:
    expected = []
    x = su.slice_dframe(df, {"budget": (25000,)})
    assert x[0].to_dict("records") == expected
    assert not x[1]


def test_build_plan():
    plan = su.build_plan(("a", "b", "c"))
    assert plan[0].output() == (("a", "b", "c"), {None: (None,)})

    # Check that condition/on pairs can be in any order:
    plan = su.build_plan(("a", "b", ("a", {"c": "x"})))

    assert plan[0].output() == (("a",), {"c": ("x",)})
    assert plan[1].output() == (("a", "b"), {None: (None,)})

    plan = su.build_plan(("a", "b", ({"c": "x"}, "a")))
    assert plan[0].output() == (("a",), {"c": ("x",)})
    assert plan[1].output() == (("a", "b"), {None: (None,)})

    plan = su.build_plan((({"c": "x"}, "a"),))
    assert plan[0].output() == (("a",), {"c": ("x",)})
    assert len(plan) == 1


def test_prep_ons():
    assert su.prep_ons("test") == ("test",)
    assert su.prep_ons((("a",), ("b", "c"))) == (("a",), ("b", "c"))
    assert su.prep_ons(["a", "b", ("c", "d")]) == ("a", "b", ("c", "d"))
    assert su.prep_ons(("a", "b")) == (("a", "b"),)


def test_prep_suffixes():
    assert su.prep_suffixes(None, 2) == ("_A", "_B")
    assert su.prep_suffixes("_x", 1) == ("_x",)
    assert su.prep_suffixes(("_x", "_y"), 2) == ("_x", "_y")

    with pytest.raises(ValueError, match="Suffix len=2, suffixes="):
        su.prep_suffixes(("_x", "_y"), 3)


class TestSupplementGuide:
    def test_basics(self):
        sg = gd.SupplementGuide("a", "b", "c", inexact=True)
        # Ensures thresholds is subscriptable:
        assert sg.thresholds[0] == 0.9

    def test_output(self):
        sg = gd.SupplementGuide("a", "b", "c", conditions={"c": "x"})
        assert sg.output() == (("a", "b", "c"), {"c": ("x",)})
        assert sg.output("on", "thresholds") == (("a", "b", "c"), None)
        assert sg.output("on") == ("a", "b", "c")
