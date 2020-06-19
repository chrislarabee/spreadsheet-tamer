from typing import Callable
import collections as col

import pandas as pd

import datagenius.util as u


class GeniusMetadata(Callable):
    @property
    def rejects(self):
        return self._rejects

    @property
    def stages(self):
        return {s: getattr(self, s) for s in self._stages}

    """
    When coupled with Genius transmutations, tracks their activity and
    provides methods for reporting out on it.
    """
    def __init__(self):
        self._rejects: pd.DataFrame = pd.DataFrame()
        self._no_stage: pd.DataFrame = pd.DataFrame(
            columns=['transmutation'])
        self._stages = ['_no_stage']

    def track(
            self,
            transmutation: Callable,
            df: pd.DataFrame,
            **kwargs) -> pd.DataFrame:
        """
        Runs the passed transmutation on the passed DataFrame with the
        passed kwargs. Collects any metadata spit out by the function
        and returns the DataFrame once changed by the transmutation.

        Args:
            transmutation: A function. If decorated as a transmutation
                function, the organization of the results can be better
                controlled.
            df: The DataFrame to execute the transformation on.
            **kwargs: The keyword args, some or none of which will be
                passed to transmutation, depending on what kwargs it
                takes.

        Returns: The passed DataFrame, as modified by the transmutation.

        """
        t_kwargs = u.align_args(transmutation, kwargs, 'df')
        result = transmutation(df, **t_kwargs)
        if isinstance(result, tuple):
            meta_result = result[1]
            result = result[0]
            metadata = meta_result.get('metadata')
            rejects = meta_result.get('rejects')
            if metadata is not None:
                metadata['transmutation'] = transmutation.__name__
                self._intake(
                    metadata, getattr(transmutation, 'stage', '_no_stage'))
                meta_result.pop('metadata')
            if rejects is not None:
                self._intake(rejects, '_rejects')
                meta_result.pop('rejects')
        return result

    def _intake(self, incoming: pd.DataFrame, attr: str) -> None:
        """
        Adds an incoming DataFrame to an attribute on GeniusMetadata
        that is also a DataFrame object.

        Args:
            incoming: The incoming DataFrame.
            attr: A string, the name of an existing attribute on the
                GeniusMetadata object, or a new one.

        Returns: None

        """
        # New attributes are assumed to be stages:
        if getattr(self, attr, None) is None:
            setattr(self, attr, pd.DataFrame(columns=['transmutation']))
            self._stages.append(attr)
        if isinstance(incoming, pd.DataFrame):
            setattr(self, attr, pd.concat(
                (getattr(self, attr), incoming)).reset_index(drop=True))

    def __call__(self, df, *transmutations, **options):
        for tm in transmutations:
            self.track(tm, df, **options)
        return df

