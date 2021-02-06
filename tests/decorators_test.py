import pandas as pd
from numpy import nan

from tamer import decorators


class TestNullable:
    def test_that_it_can_wrap_a_function(self):
        @decorators.nullable
        def _func(x):
            return x[0]

        assert _func([1, 2, 3]) == 1
        assert pd.isna(_func(nan))
