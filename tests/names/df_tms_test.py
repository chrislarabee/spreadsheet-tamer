import pytest
import pandas as pd

import datagenius.names.df_tms as tms
from datagenius.config import config


class TestParseNameStringColumn:
    @pytest.fixture
    def simple_namestrings(self):
        return pd.DataFrame(
            [
                [1, "Ewan Hudson"],
                [2, "Finley Chambers"],
                [3, "Harley D. Patel"],
                [4, "Dr. Jaden Blackburn"],
                [5, "Mr. Alex J. White, III"],
            ],
            columns=["id", "name"],
        )

    def test_that_it_can_handle_a_dataframe(self, simple_namestrings):
        expected = pd.DataFrame(
            [
                [None, "Ewan", None, "Hudson", None, True],
                [None, "Finley", None, "Chambers", None, True],
                [None, "Harley", "D.", "Patel", None, True],
                ["Dr.", "Jaden", None, "Blackburn", None, True],
                ["Mr.", "Alex", "J.", "White", "Iii", True],
            ],
            columns=[
                "prefix",
                "fname",
                "mname",
                "lname",
                "suffix",
                "valid",
            ],
        )
        expected = simple_namestrings.join(expected)
        df = tms.parse_name_string_column(simple_namestrings, "name")
        pd.testing.assert_frame_equal(df, expected)

    def test_that_it_responds_to_custom_config(self, monkeypatch, simple_namestrings):
        monkeypatch.setattr(config, "name_column_labels", ("a", "b", "c", "d", "e"))
        df = tms.parse_name_string_column(simple_namestrings, "name")
        assert list(df.columns) == ["id", "name", "a", "b", "c", "d", "e", "valid"]
