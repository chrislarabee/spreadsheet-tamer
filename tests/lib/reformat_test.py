import pandas as pd

import datagenius.lib.reformat as rf


def test_reformat_df(products, formatted_products):
    template = ["Prod Id", "Name", "Price", "Cost", "Prod UPC", "Material", "Barcode"]
    mapping = dict(
        id="Prod Id",
        name="Name",
        price="Price",
        cost="Cost",
        upc=("Prod UPC", "Barcode"),
        attr1="Material",
    )
    df = pd.DataFrame(**products)
    df, md_dict = rf.reformat_df(df, template, mapping)
    pd.testing.assert_frame_equal(df, pd.DataFrame(**formatted_products))
    expected_metadata = pd.DataFrame(
        [
            dict(
                id="Prod Id",
                name="Name",
                price="Price",
                cost="Cost",
                upc="Prod UPC,Barcode",
                attr1="Material",
                attr2=3,
                attr3=0,
                attr4=0,
                attr5=0,
            )
        ]
    )
    pd.testing.assert_frame_equal(md_dict["metadata"], expected_metadata)
    assert md_dict["orig_header"] == template


def test_fill_defaults(products):
    df = pd.DataFrame(**products)
    df, md_dict = rf.fill_defaults(df, dict(attr1="plastic", attr5="taxable"))
    pd.testing.assert_series_equal(
        df["attr1"], pd.Series(["plastic", "copper", "steel", "aluminum"], name="attr1")
    )
    pd.testing.assert_series_equal(
        df["attr5"],
        pd.Series(["taxable", "taxable", "taxable", "taxable"], name="attr5"),
    )
    expected_metadata = pd.DataFrame(
        [
            dict(
                id=0,
                name=0,
                price=0,
                cost=0,
                upc=0,
                attr1=1,
                attr2=0,
                attr3=0,
                attr4=0,
                attr5=4,
            )
        ]
    )
    pd.testing.assert_frame_equal(md_dict["metadata"], expected_metadata)
