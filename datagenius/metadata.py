from typing import Callable
import collections as col

import pandas as pd

import datagenius.util as u


class GeniusMetadata(Callable):
    @property
    def rejects(self):
        return self._rejects

    @property
    def reject_ct(self):
        return self._rejects.shape[0]

    @property
    def collected(self):
        # Used to store collected metadata on transmutation results.
        return self._collected

    @property
    def output_header(self):
        # Used to store the header that will be used for the final
        # output file, with caps and spaces and such.
        return self._output_header

    """
    When coupled with Genius transmutations, tracks their activity and
    provides methods for reporting out on it.
    """

    def __init__(self):
        self._rejects: pd.DataFrame = pd.DataFrame()
        self._collected: pd.DataFrame = pd.DataFrame(columns=["stage", "transmutation"])
        self._output_header = []

    def track(self, transmutation: Callable, df: pd.DataFrame, **kwargs) -> tuple:
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
        t_kwargs = u.align_args(transmutation, kwargs, "df")
        print(f"[DATAGENIUS]: Applying {transmutation.__name__}...")
        result = transmutation(df, **t_kwargs)
        if isinstance(result, tuple):
            meta_result = result[1]
            result = result[0]
            metadata = meta_result.get("metadata")
            rejects = meta_result.get("rejects")
            new_kwargs = meta_result.get("new_kwargs")
            o_header = meta_result.get("orig_header")
            if metadata is not None:
                metadata["transmutation"] = transmutation.__name__
                stage = getattr(transmutation, "stage", "_no_stage")
                metadata["stage"] = stage
                self._intake(metadata, "_collected")
                meta_result.pop("metadata")
            if rejects is not None:
                self._intake(rejects, "_rejects")
                meta_result.pop("rejects")
            if new_kwargs is not None:
                kwargs = {**kwargs, **new_kwargs}
            if o_header is not None:
                self._output_header = o_header
        return result, kwargs

    def combine(self, other):
        """
        Combines the data in this GeniusMetadata object with the data
        from another GeniusMetadata object. Transmutations tracked by
        both GeniusMetadata objects will have their totals aggregated.

        Args:
            other: A GeniusMetadata object.

        Returns: None

        """
        if isinstance(other, GeniusMetadata):
            self._collected = self._collected.append(other.collected)
            self._collected = (
                self._collected.groupby(["stage", "transmutation"]).sum().reset_index()
            )
            self._rejects = self._rejects.append(other.rejects)
            new_columns = set(self._output_header).difference(set(other._output_header))
            self._output_header += list(new_columns)
        else:
            raise TypeError(
                f"GeniusMetadata.combine method can only accept other "
                f"GeniusMetadata objects. Passed object type = "
                f"{type(other)}"
            )

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
        if isinstance(incoming, pd.DataFrame):
            setattr(
                self,
                attr,
                pd.concat((getattr(self, attr), incoming)).reset_index(drop=True),
            )

    def __call__(self, df, *transmutations, **options) -> pd.DataFrame:
        """
        Tracks the results of any number of passed transmutations.

        Args:
            df: The DataFrame to execute each transmutation function
                on.
            *transmutations: Any number of transmutation functions.
            **options: Keyword args that might be used by any of the
                transmutations.

        Returns: The DataFrame, altered by the passed transmutations.

        """
        for tm in transmutations:
            df, options = self.track(tm, df, **options)
        return df
