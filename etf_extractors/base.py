from __future__ import annotations

from abc import ABC, abstractmethod
from pathlib import Path
import pandas as pd


class BaseETFExtractor(ABC):
    def __init__(self, url: str, output_dir: str = "out"):
        self.url = url
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)

    @property
    @abstractmethod
    def source_name(self) -> str:
        pass

    @property
    @abstractmethod
    def fund_code(self) -> str:
        pass

    @abstractmethod
    def extract(self) -> pd.DataFrame:
        pass

    def save(self, df: pd.DataFrame, fmt: str = "csv") -> Path:
        output_file = self.output_dir / f"{self.fund_code}.{fmt}"

        if fmt == "csv":
            df.to_csv(output_file, index=False)
        elif fmt == "json":
            df.to_json(output_file, orient="records", indent=2, force_ascii=False)
        else:
            raise ValueError("fmt must be csv or json")

        return output_file