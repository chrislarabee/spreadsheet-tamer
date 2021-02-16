import os
from typing import Callable, Dict, List, Any
import warnings

import pytest
from numpy import nan

from datagenius.io.text import SheetsAPI
from tests import testing_tools
import tamer.config as config


# SheetsAPI test marker
def pytest_addoption(parser):
    parser.addoption(
        "--run-sheets-tests",
        action="store_true",
        default=False,
        help="Include SheetsAPI-based tests in the test session.",
    )


def pytest_configure(config):
    config.addinivalue_line(
        "markers", "sheets_api: mark test as a SheetsAPI-based test"
    )


def pytest_collection_modifyitems(config, items):
    if config.getoption("--run-sheets-tests"):
        return
    skip_sheets_tests = pytest.mark.skip(reason="need --run-sheets-tests option to run")
    for item in items:
        if "sheets_api" in item.keywords:
            item.add_marker(skip_sheets_tests)


@pytest.fixture(scope="session")
def sheets_api():
    if os.path.exists("token.pickle") or os.path.exists("credentials.json"):
        s = SheetsAPI()
        yield s
        print("\n-- Cleaning up google drive objects created for tests...")
        ids = testing_tools.created_ids
        for i in ids:
            s.delete_object(i)
        print(f"-- Successfully cleaned up {len(ids)} objects.")
    else:
        warnings.warn(
            f"No credentials.json or token.pickle found in "
            f"{os.getcwd()}. The SheetsAPI is not being tested. "
            f"To execute all tests properly download google api "
            f"credentials as described in the README."
        )
        yield None


@pytest.fixture
def customers():
    def _gen_customers(f: Callable = str) -> Dict[str, List[Any]]:
        """
        Use to generate this DataFrame (with DataFrame(**customers)):
            id  fname       lname       foreign_key
        0   1   Yancy       Cordwainer  00025
        1   2   Muhammad    El-Kanan    00076
        2   3   Luisa       Romero      00123
        3   4   Semaj       Soto        01234

        Args:
            f (Callable, optional): A type object to wrap id values in. Defaults
                to str.

        Returns:
            Dict[str, List[Any]]: A dictionary with a list assigned to each key.
        """
        d = dict(
            columns=["id", "fname", "lname", "foreign_key"],
            data=[
                [f(1), "Yancy", "Cordwainer", "00025"],
                [f(2), "Muhammad", "El-Kanan", "00076"],
                [f(3), "Luisa", "Romero", "00123"],
                [f(4), "Semaj", "Soto", "01234"],
            ],
        )
        return d

    return _gen_customers


@pytest.fixture
def sales():
    return dict(
        columns=["location", "region", "sales"],
        data=[
            ["Bayside Store", "Northern", 500],
            ["West Valley Store", "Northern", 300],
            ["Precioso Store", "Southern", 1000],
            ["Kalliope Store", "Southern", 200],
        ],
    )


@pytest.fixture
def regions():
    return dict(
        columns=["region", "stores", "employees"],
        data=[["Northern", 50, 500], ["Southern", 42, 450]],
    )


@pytest.fixture
def stores():
    return dict(
        columns=["location", "region", "budget", "inventory"],
        data=[
            ["Bayside", "Northern", 100000, 5000],
            ["W Valley", "Northern", 90000, 4500],
            ["Precioso", "Southern", 110000, 4500],
            ["Kalliope", "Southern", 90000, 4500],
        ],
    )


@pytest.fixture
def products():
    return dict(
        columns=[
            "id",
            "name",
            "price",
            "cost",
            "upc",
            "attr1",
            "attr2",
            "attr3",
            "attr4",
            "attr5",
        ],
        data=[
            [1, "Widget", 8.5, 4.0, 1234567890, nan, nan, nan, nan, nan],
            [2, "Doohickey", 9.99, 5.0, 2345678901, "copper", "large", nan, nan, nan],
            [3, "Flange", 1.0, 0.2, 3456789012, "steel", "small", nan, nan, nan],
            [4, "Whatsit", 5.0, 2.0, 4567890123, "aluminum", "small", nan, nan, nan],
        ],
    )


@pytest.fixture
def employees():
    return dict(
        columns=["employee_id", "department", "name", "wfh_stipend"],
        data=[
            [1, "Sales", "Aidan Kelly", nan],
            [2, "Sales", "Natasha Doyle", 1000],
            [3, "Customer Service", "Callum Mays", 1000],
            [4, "Customer Service", "Jazlynn Monroe", nan],
        ],
    )


@pytest.fixture
def formatted_products():
    return dict(
        columns=["prod_id", "name", "price", "cost", "prod_upc", "material", "barcode"],
        data=[
            [1, "Widget", 8.5, 4.0, 1234567890, nan, 1234567890],
            [2, "Doohickey", 9.99, 5.0, 2345678901, "copper", 2345678901],
            [3, "Flange", 1.0, 0.2, 3456789012, "steel", 3456789012],
            [4, "Whatsit", 5.0, 2.0, 4567890123, "aluminum", 4567890123],
        ],
    )


@pytest.fixture
def gaps():
    """
    Generates a dataset with gap rows from bad formatting for
    testing.

    Returns: A list of lists.

    """
    return [
        [nan, nan, nan, nan],
        [nan, nan, nan, nan],
        [nan, nan, nan, nan],
        [nan, nan, nan, nan],
        ["id", "fname", "lname", "foreign_key"],
        [nan, nan, nan, nan],
        ["1", "Yancy", "Cordwainer", "00025"],
        ["2", "Muhammad", "El-Kanan", "00076"],
        ["3", "Luisa", "Romero", "00123"],
        ["4", "Semaj", "Soto", "01234"],
    ]


@pytest.fixture
def gaps_totals():
    """
    Generates a 'report-like' dataset that has sub-total rows and
    some titles and sub-titles and such for testing.

    Returns: A list of lists.

    """

    def _gen(w_gaps=True, w_pre_header=True):
        ph = [["Sales by Location Report", nan, nan], ["Grouping: Region", nan, nan]]
        g = [[nan, nan, nan], [nan, nan, nan]]
        x = [
            ["location", "region", "sales"],
            ["Bayside Store", "Northern", 500],
            ["West Valley Store", "Northern", 300],
            [nan, nan, 800],
            ["Precioso Store", "Southern", 1000],
            ["Kalliope Store", "Southern", 200],
            [nan, nan, 1200],
        ]
        y = []
        if w_pre_header:
            y = ph
        if w_gaps:
            y = [*y, *g]
        return [*y, *x]

    return _gen


@pytest.fixture
def needs_cleanse_totals():
    return dict(
        columns=["location", "region", "sales"],
        data=[
            ["Bayside Store", "Northern", 500],
            ["West Valley Store", "Northern", 300],
            [nan, nan, 800],
            ["Precioso Store", "Southern", 1000],
            ["Kalliope Store", "Southern", 200],
            [nan, nan, 1200],
        ],
    )


@pytest.fixture
def needs_extrapolation():
    return dict(
        columns=["employee_id", "department", "name", "wfh_stipend"],
        data=[
            [1, "Sales", "Aidan Kelly", nan],
            [2, nan, "Natasha Doyle", 1000],
            [3, "Customer Service", "Callum Mays", 1000],
            [4, nan, "Jazlynn Monroe", nan],
        ],
    )


@pytest.fixture
def needs_cleanse_typos():
    return dict(
        columns=[
            "id",
            "name",
            "price",
            "cost",
            "upc",
            "attr1",
            "attr2",
            "attr3",
            "attr4",
            "attr5",
        ],
        data=[
            [1, "Widget", 8.5, 4.0, 1234567890, nan, nan, nan, nan, nan],
            [2, "Doohickey", 9.99, 5.0, 2345678901, "cu", "large", nan, nan, nan],
            [3, "Flange", 1.0, 0.2, 3456789012, "steel", "sm", nan, nan, nan],
            [4, "Whatsit", 5.0, 2.0, 4567890123, "aluminum", "s", nan, nan, nan],
        ],
    )


@pytest.fixture
def df_for_formulas():
    return [
        dict(col1=20, col2=10, col3=30),
        dict(col1=100, col2=50, col3=200),
        dict(col1=1000, col2=200, col3=754),
    ]
