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
