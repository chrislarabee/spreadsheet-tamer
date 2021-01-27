from pathlib import Path

import pytest

import datagenius.names.util as u
import datagenius.config as config


class TestLoadPatterns:
    def test_load_w_out_custom(self):
        result = u.load_patterns()
        assert isinstance(result, dict)

    def test_load_w_custom(self, monkeypatch, mocker):
        load_custom_pattern_mock = mocker.Mock(u.load_custom_pattern)
        load_custom_pattern_mock.return_value = dict(
            compound_fnames=['this', 'is', 'a', 'test']
        )
        monkeypatch.setattr(u, 'load_custom_pattern', load_custom_pattern_mock)
        monkeypatch.setattr(config, 'custom_pattern_file', 'test')
        result = u.load_patterns()
        assert 'this' in result['compound_fnames']
        assert 'is' in result['compound_fnames']
        assert 'a' in result['compound_fnames']
        assert 'test' in result['compound_fnames']
        

class TestLoadCustomPattern():
    def test_that_it_loads(self):
        result = u.load_custom_pattern(Path('tests/samples/custom_pattern.yml'))
        assert result == dict(compound_fnames=['this', 'is', 'a', 'test'])

    def test_that_it_errors_on_non_yaml(self):
        with pytest.raises(
            ValueError, 
            match='custom_pattern_file tests/samples/custom_pattern.csv must be a .yml '
        ):
            u.load_custom_pattern(Path('tests/samples/custom_pattern.csv'))

    def test_that_it_errors_on_improper_contents(self):
        with pytest.raises(
            ValueError, 
            match="custom_pattern_file must contain only list objects. {'a': 'x', "
        ):
            u.load_custom_pattern(Path('tests/samples/bad_pattern.yml'))
        