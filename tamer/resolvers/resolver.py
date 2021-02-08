from abc import ABC, abstractmethod

import pandas as pd


class Resolver(ABC):
    @abstractmethod
    def resolve(self, df: pd.DataFrame) -> pd.DataFrame:
        pass
