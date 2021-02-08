from typing import Tuple, Optional

import pandas as pd

from .resolver import Resolver
from ..decorators import resolution
from ..header import Header
from ..strings import util as su

class Preprocessor(Resolver):
    def __init__(self) -> None:
        super().__init__()
    
    def resolve(self, df: pd.DataFrame) -> pd.DataFrame:
        return super().resolve(df)

    @staticmethod
    @resolution
    def _detect_header(df: pd.DataFrame, manual_header: Header = None) -> Tuple[pd.DataFrame, Optional[int]]:
        """
        Takes a pandas DataFrame and sets its column names to be the values of 
        the first row containing all true strings and removes that row from the 
        DataFrame.
        -
        Args:
            df (pd.DataFrame): The DataFrame to check for a valid header row.
            manual_header (Header, optional): A manually supplied Header which 
                will be used instead of attempting to detect a valid header. 
                Defaults to None.
        -
        Returns:
            Tuple[pd.DataFrame, Optional[int]]: The DataFrame, as well as the
                index of the row where the header was located, if one was.
        """
        header_idx = None
        if manual_header:
            df.columns = manual_header
        else:
            true_str_series = df.apply(lambda x: su.count_true_str(x) == len(x), axis=1)
            first_idx = next((i for i, v in true_str_series.items() if v), None)
            if first_idx is not None:
                df.columns = Header(df.iloc[first_idx])
                header_idx = first_idx
                df = df.drop(index=first_idx).reset_index(drop=True)
        return df, header_idx

    @staticmethod
    @resolution
    def _purge_pre_header(df: pd.DataFrame, header_idx: int = None) -> pd.DataFrame:
        """
        Removes any rows that appear before the header row in a DataFrame where 
        the header row wasn't the first row in the source data. Purged rows are 
        stored in spreadsheet-tamer's metadata.
        -
        Args:
            df (pd.DataFrame): The DataFrame to purge rows from.
            header_idx (int, optional): The index of the header row. Defaults to 
                None.
        -
        Returns:
            pd.DataFrame: The DataFrame, with any rows appearing before the 
                header removed.
        """
        if header_idx:
            metadata = dict()
            if header_idx > 0:
                rejects = df.iloc[:header_idx]
                metadata["rejects"] = rejects
                metadata["metadata"] = pd.DataFrame(rejects.count()).T
            df.drop(index=[i for i in range(header_idx)], inplace=True)
            df.reset_index(drop=True, inplace=True)
            return df, metadata
        else:
            return df