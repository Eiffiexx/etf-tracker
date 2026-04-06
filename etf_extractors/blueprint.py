from __future__ import annotations

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

        # Unified final schema
        final_df = pd.DataFrame({
            "source": df["source"],
            "fund": df["fund"],
            "as_of_date": df["as_of_date"] if "as_of_date" in df.columns else None,
            "name": df["name"] if "name" in df.columns else None,
            "ticker": df["ticker"] if "ticker" in df.columns else None,
            "identifier": df["identifier"] if "identifier" in df.columns else None,
            "weight_pct": df["weight_pct"] if "weight_pct" in df.columns else None,
            "shares": df["shares"] if "shares" in df.columns else None,
            "market_value": df["market_value"] if "market_value" in df.columns else None,
        })

        return final_df