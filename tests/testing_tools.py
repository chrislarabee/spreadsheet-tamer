import pytest

from datagenius.io.text import SheetsAPI

# Used to log google drive objects as they are created so they can be
# deleted after tests are run.
created_ids = []


def check_sheets_api_skip(s_api: SheetsAPI):
    """
    Used by tests that need the sheets_api fixture. If the fixture
    cannot be instantiated then the test will be skipped.

    Args:
        s_api: A SheetsAPI object.

    Returns: None

    """
    if not s_api:
        pytest.skip('No credentials set up for google api.')
