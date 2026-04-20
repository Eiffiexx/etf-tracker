from __future__ import annotations

import re

import pandas as pd

from etf_extractors.base import BaseETFExtractor
from etf_extractors.utils import fetch_html, find_csv_link, read_csv_from_url


class BlueprintExtractor(BaseETFExtractor):
    @property
    def source_name(self) -> str:
        return "blueprint"

    @property
    def fund_code(self) -> str:
        return "tfpn"

    def extract(self) -> pd.DataFrame:
        html = fetch_html(self.url)
        csv_url = find_csv_link(self.url, html)

        if not csv_url:
            raise RuntimeError("Could not find CSV link on Blueprint page.")

        raw_df = read_csv_from_url(csv_url)

        column_map = {
            "Date": "as_of_date",
            "Account": "account",
            "StockTicker": "ticker",
            "CUSIP": "identifier",
            "SecurityName": "name",
            "Shares": "shares",
            "Price": "price",
            "MarketValue": "market_value",
            "Weightings": "weight_pct",
            "NetAssets": "net_assets",
            "SharesOutstanding": "shares_outstanding",
            "CreationUnits": "creation_units",
            "MoneyMarketFlag": "money_market_flag",
        }

        df = raw_df.rename(columns=column_map).copy()

        df["source"] = self.source_name
        df["fund"] = self.fund_code.upper()

        # -------- clean numeric fields --------
        if "weight_pct" in df.columns:
            df["weight_pct"] = (
                df["weight_pct"]
                .astype(str)
                .str.replace("%", "", regex=False)
                .str.replace(",", "", regex=False)
                .replace({"nan": None, "": None})
            )
            df["weight_pct"] = pd.to_numeric(df["weight_pct"], errors="coerce")

        numeric_cols = [
            "shares",
            "price",
            "market_value",
            "net_assets",
            "shares_outstanding",
            "creation_units",
        ]

        for col in numeric_cols:
            if col in df.columns:
                df[col] = (
                    df[col]
                    .astype(str)
                    .str.replace(",", "", regex=False)
                    .replace({"nan": None, "": None})
                )
                df[col] = pd.to_numeric(df[col], errors="coerce")

        # -------- normalize ticker / identifier --------
        if "ticker" in df.columns:
            df["ticker"] = df["ticker"].astype(str).str.strip()
        else:
            df["ticker"] = None

        if "identifier" in df.columns:
            df["identifier"] = df["identifier"].astype(str).str.strip()
        else:
            df["identifier"] = None

        df["normalized_ticker"] = df["ticker"].apply(self._normalize_ticker)

        # -------- classify instrument --------
        df["instrument_type"] = df.apply(self._classify_instrument, axis=1)

        # optional helper columns
        df["option_type"] = df.apply(self._extract_option_type, axis=1)
        df["is_derivative"] = df["instrument_type"].isin(["future", "option"])

        final_cols = [
            "source",
            "fund",
            "as_of_date",
            "ticker",
            "normalized_ticker",
            "identifier",
            "name",
            "instrument_type",
            "option_type",
            "is_derivative",
            "shares",
            "price",
            "market_value",
            "weight_pct",
        ]

        final_cols = [c for c in final_cols if c in df.columns]
        return df[final_cols]

    @staticmethod
    def _normalize_ticker(value: str) -> str | None:
        if value is None:
            return None

        text = str(value).strip()
        if text == "" or text.lower() == "nan":
            return None

        # remove suffixes commonly seen in futures/index rows
        suffixes = [" COMDTY", " INDEX"]
        for suffix in suffixes:
            if text.endswith(suffix):
                return text[: -len(suffix)].strip()

        return text

    @staticmethod
    def _classify_instrument(row: pd.Series) -> str:
        ticker = str(row.get("ticker", "")).upper()
        name = str(row.get("name", "")).upper()
        identifier = str(row.get("identifier", "")).upper()

        # cash
        if "CASH" in name:
            return "cash"

        # options: examples like "ADSK US 06/18/26 P250"
        if re.search(r"\s[P|C]\d+(\.\d+)?\b", name):
            return "option"

        # futures
        if "COMDTY" in ticker or "FUT" in name:
            return "future"

        # index futures / index products
        if "INDEX" in ticker:
            return "index"

        # fallback
        return "equity"

    @staticmethod
    def _extract_option_type(row: pd.Series) -> str | None:
        name = str(row.get("name", "")).upper()

        if re.search(r"\sP\d+(\.\d+)?\b", name):
            return "put"

        if re.search(r"\sC\d+(\.\d+)?\b", name):
            return "call"

        return None