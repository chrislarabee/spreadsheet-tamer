import pytest
import pandas as pd
from numpy import nan

from tamer import decorators
from tamer import metadata
from tamer import config


class TestNullable:
    def test_that_it_can_wrap_a_function(self):
        @decorators.nullable
        def _func(x):
            return x[0]

        assert _func([1, 2, 3]) == 1
        assert pd.isna(_func(nan))


class TestResolution:
    @pytest.fixture
    def md(self, monkeypatch, mocker):
        md = mocker.Mock(metadata.Metadata)
        monkeypatch.setattr(metadata, "METADATA", md)
        return md

    @pytest.fixture(autouse=True)
    def override_test_config(self, monkeypatch):
        monkeypatch.setattr(config.config, "env", "prod")

    def test_that_it_can_wrap_a_function_with_no_md_return(self, md):
        @decorators.resolution
        def _func(x):
            return x[0]

        assert _func([1, 2, 3]) == 1
        md.collect.assert_not_called()

    def test_that_it_logs_results_to_metadata_when_provided(
        self,
        md,
    ):
        @decorators.resolution
        def _func(x):
            return x[0], dict(metadata=x)

        _func([1, 2, 3])
        md.collect.assert_called_once_with("_func", metadata=[1, 2, 3])
