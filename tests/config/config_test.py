import pytest

from tamer.config.config import Config


class TestConfigNameColumnLabels:
    @pytest.fixture
    def config_test(self):
        return Config()

    def test_that_it_must_be_a_tuple(self, config_test):
        with pytest.raises(ValueError, match="Passed value type is <class 'list'>"):
            config_test.name_column_labels = ["a", "b", "c", "d", "e"]

    def test_that_it_must_be_length_5(self, config_test):
        with pytest.raises(ValueError, match="must be a tuple of length 5"):
            config_test.name_column_labels = ("a", "b", "c")

    def test_that_values_must_be_strings(self, config_test):
        with pytest.raises(ValueError, match="1 is type <class 'int'>"):
            config_test.name_column_labels = ("a", "b", 1, "d", "e")
