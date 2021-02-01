from pathlib import Path

import pytest

from datagenius.gconfig import Patterns, GConfig


class TestLoadPatterns:
    def test_that_it_can_load_patterns(self):
        result = Patterns._load_patterns()
        assert isinstance(result, dict)

    def test_that_it_can_update_patterns(self):
        p = Patterns()
        p._compound_fnames = []
        p._update_patterns(compound_fnames=["this", "is", "a", "test"])
        assert p.compound_fnames == ["this", "is", "a", "test"]

    def test_get_invalid_chars(self):
        result = Patterns._get_invalid_chars()
        assert "!" in result
        assert set(result) - {"&", "-", ".", "'"} == set(result)


class TestLoadCustomPattern:
    def test_that_it_loads(self):
        result = Patterns._load_custom_pattern(Path("tests/samples/custom_pattern.yml"))
        assert result == dict(compound_fnames=["this", "is", "a", "test"])

    def test_that_it_errors_on_non_yaml(self):
        with pytest.raises(
            ValueError,
            match="custom_pattern_file tests/samples/custom_pattern.csv must be a .yml ",
        ):
            Patterns._load_custom_pattern(Path("tests/samples/custom_pattern.csv"))

    def test_that_it_errors_on_improper_contents(self):
        with pytest.raises(
            ValueError,
            match="custom_pattern_file must contain only list objects. {'a': 'x', ",
        ):
            Patterns._load_custom_pattern(Path("tests/samples/bad_pattern.yml"))


class TestGconfigNameColumnLabels:
    @pytest.fixture
    def gconfig(self):
        return GConfig()

    def test_that_it_must_be_a_tuple(self, gconfig):
        with pytest.raises(ValueError, match="Passed value type is <class 'list'>"):
            gconfig.name_column_labels = ["a", "b", "c", "d", "e"]

    def test_that_it_must_be_length_5(self, gconfig):
        with pytest.raises(ValueError, match="must be a tuple of length 5"):
            gconfig.name_column_labels = ("a", "b", "c")

    def test_that_values_must_be_strings(self, gconfig):
        with pytest.raises(ValueError, match="1 is type <class 'int'>"):
            gconfig.name_column_labels = ("a", "b", 1, "d", "e")
