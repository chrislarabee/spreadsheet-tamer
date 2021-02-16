from typing import Tuple, Optional

import pandas as pd
from numpy import nan

from .resolver import Resolver
from ..decorators import resolution
from ..header import Header
from ..strings import util as su
from .. import metadata as md
from .. import iterutils
from ..type_handling import CollectibleMetadata


class Preprocessor(Resolver):
    def __init__(self, manual_header: Header = None) -> None:
        """
        Applies essentially mandatory preprocessing resolutions to the passed
        DataFrame. This Resolver is used as part of spreadsheet-tamer's io
        functionality, so probably doesn't need to be instantiated and called
        directly unless your data is coming from a source spreadsheet-tamer can't
        read.

        Args:
            manual_header (Header, optional): A Header to use in place of
                attempting to detect a header. Defaults to None.
        """
        self._manual_header = manual_header
        super().__init__()

    def resolve(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Applies Preprocessor's resolutions.

        Args:
            df (pd.DataFrame): The DataFrame to apply preprocessing resolutions
                to.

        Returns:
            pd.DataFrame: The DataFrame, with any whitespace and header issues
                resolved.
        """
        df, _ = self._normalize_whitespace(df)
        df = self._purge_gap_rows(df)
        if iterutils.withinplus(df.columns, r"[Uu]nnamed:*[ _]\d") or isinstance(
            df.columns, pd.RangeIndex
        ):
            df, header_idx = self._detect_header(df, self._manual_header)
            df, _ = self._purge_pre_header(df, header_idx=header_idx)
        return df

    @staticmethod
    @resolution
    def _detect_header(
        df: pd.DataFrame, manual_header: Header = None
    ) -> Tuple[pd.DataFrame, Optional[int]]:
        """
        Takes a pandas DataFrame and sets its column names to be the values of
        the first row containing all true strings and removes that row from the
        DataFrame.

        Args:
            df (pd.DataFrame): The DataFrame to check for a valid header row.
            manual_header (Header, optional): A manually supplied Header which
                will be used instead of attempting to detect a valid header.
                Defaults to None.

        Returns:
            Tuple[pd.DataFrame, Optional[int]]: The DataFrame, as well as the
                index of the row where the header was located, if one was.
        """
        header_idx = None
        if manual_header:
            df.columns = manual_header
        else:
            true_str_series = df.apply(lambda x: su.count_true_str(x) == len(x), axis=1)
            for i, v in true_str_series.items():
                if v:
                    first_idx = i
            else:
                first_idx = None
            # This one line version is pretty but messes up the type checker. :(
            # first_idx = next((i for i, v in true_str_series.items() if v), None)
            if first_idx is not None:
                df.columns = Header(df.iloc[first_idx])
                header_idx = first_idx
                df = pd.DataFrame(df.drop(index=first_idx).reset_index(drop=True))
        return df, header_idx

    @staticmethod
    @resolution
    def _normalize_whitespace(
        df: pd.DataFrame,
    ) -> Tuple[pd.DataFrame, CollectibleMetadata]:
        """
        A simple resolution that applies string.utils.clean_whitespace to every
        cell in a DataFrame.

        Args:
            df (pd.DataFrame): DataFrame to ensure whitespace is normalized.

        Returns:
            pd.DataFrame: The DataFrame, with any string values cleansed of
                excess whitespace.
        """
        md_df = md.gen_empty_md_df(df.columns)
        for c in df.columns:
            result = df[c].apply(su.clean_whitespace)
            # Pass the index in case the DataFrame is being chunked on read:
            result = pd.DataFrame(result.to_list(), index=df.index)
            df[c] = result[1]
            md_df[c] = result[0].sum()
        return df, {"metadata": md_df}

    @staticmethod
    @resolution
    def _purge_pre_header(
        df: pd.DataFrame, header_idx: int = None
    ) -> Tuple[pd.DataFrame, CollectibleMetadata]:
        """
        Removes any rows that appear before the header row in a DataFrame where
        the header row wasn't the first row in the source data. Purged rows are
        stored in spreadsheet-tamer's metadata.

        Args:
            df (pd.DataFrame): The DataFrame to purge rows from.
            header_idx (int, optional): The index of the header row. Defaults to
                None.

        Returns:
            pd.DataFrame: The DataFrame, with any rows appearing before the
                header removed.
        """
        metadata = dict()
        if header_idx:
            if header_idx > 0:
                rejects = df.iloc[:header_idx]
                metadata["rejects"] = rejects
                metadata["metadata"] = pd.DataFrame(rejects.count()).T
            df.drop(index=[i for i in range(header_idx)], inplace=True)
            df.reset_index(drop=True, inplace=True)
        return df, metadata

    @staticmethod
    @resolution
    def _purge_gap_rows(df: pd.DataFrame) -> pd.DataFrame:
        """
        Removes any rows that have only nan or blank str ('') values.

        Args:
            df (pd.DataFrame): The DataFrame to purge rows from.

        Returns:
            pd.DataFrame: The DataFrame, with any gap rows removed.
        """
        df.replace("", nan, inplace=True)
        df = df.dropna(how="all")
        # reset_index returns DataFrame | None based on inplace, pyright dislikes that.
        df = df.reset_index(drop=True)  # type: ignore
        return df
