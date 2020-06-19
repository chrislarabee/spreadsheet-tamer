from typing import Callable
import collections as col

import pandas as pd

import datagenius.util as u


class GeniusMetadata(Callable):
    """
    When coupled with Genius transmutations, tracks their activity and
    provides methods for reporting out on it.
    """
    def __init__(self):
        self._transformations: dict = dict()
        self._transmutations: dict = dict()

    def track(self, transmutation: Callable, df: pd.DataFrame, **kwargs):
        """
        Runs the passed transmutation on the passed DataFrame with the
        passed kwargs. Collects any metadata spit out by the function
        and returns the DataFrame once changed by the transmutation.

        Args:
            transmutation:
            df:
            **kwargs:

        Returns:

        """
        t_kwargs = u.align_args(transmutation, kwargs, 'df')
        result = transmutation(df, **t_kwargs)
        if isinstance(result, tuple):
            self._transmutations[transmutation.__name__] = result[1]
            return result[0]
        else:
            return result

    @staticmethod
    def _parse_tm_output(tm_output: tuple, func: Callable) -> dict:
        result = col.OrderedDict(
            df=tm_output[0], col_md=None, rejects=None)
        start = 1 + func.no_metadata
        start += not func.collects_rejects
        for i, o in enumerate(tm_output, 1):
            pass



    def __call__(self, df, *transmutations, **options):
        for tm in transmutations:
            self.track(tm, df, **options)
        return df

