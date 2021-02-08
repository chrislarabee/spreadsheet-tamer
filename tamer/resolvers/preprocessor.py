import pandas as pd

from .resolver import Resolver
from ..decorators import resolution


class Preprocessor(Resolver):
    def __init__(self) -> None:
        super().__init__()
    
    def resolve(self, df: pd.DataFrame) -> pd.DataFrame:
        return super().resolve(df)

    @staticmethod
    @resolution
    def _purge_pre_header(df: pd.DataFrame, header_idx: int = None) -> pd.DataFrame:
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