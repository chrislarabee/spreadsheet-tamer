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
