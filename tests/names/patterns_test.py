from pathlib import Path

import pytest

from datagenius.names.patterns import Patterns


class TestLoadPatterns:
    def test_that_it_can_load_patterns(self):
        result = Patterns.load_patterns()
        assert isinstance(result, dict)

    def test_that_it_can_update_patterns(self):
        p = Patterns()
        p._compound_fnames = []
        p._update_patterns(compound_fnames=['this', 'is', 'a', 'test'])
        assert p.compound_fnames == ['this', 'is', 'a', 'test']
    
    def test_get_invalid_chars(self):
        result = Patterns._get_invalid_chars()
        assert '!' in result
        assert set(result) - {'&', '-', '.', "'"} == set(result)


class TestLoadCustomPattern:
    def test_that_it_loads(self):
        result = Patterns._load_custom_pattern(Path('tests/samples/custom_pattern.yml'))
        assert result == dict(compound_fnames=['this', 'is', 'a', 'test'])

    def test_that_it_errors_on_non_yaml(self):
        with pytest.raises(
            ValueError, 
            match='custom_pattern_file tests/samples/custom_pattern.csv must be a .yml '
        ):
            Patterns._load_custom_pattern(Path('tests/samples/custom_pattern.csv'))

    def test_that_it_errors_on_improper_contents(self):
        with pytest.raises(
            ValueError, 
            match="custom_pattern_file must contain only list objects. {'a': 'x', "
        ):
            Patterns._load_custom_pattern(Path('tests/samples/bad_pattern.yml'))
