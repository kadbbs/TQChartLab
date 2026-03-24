from __future__ import annotations

from abc import ABC, abstractmethod

import pandas as pd


class DataSource(ABC):
    provider_name: str

    @abstractmethod
    def start(self) -> None:
        raise NotImplementedError

    @abstractmethod
    def stop(self) -> None:
        raise NotImplementedError

    @abstractmethod
    def get_bars(self) -> pd.DataFrame:
        raise NotImplementedError

    def configure(self, **kwargs) -> None:
        return None
