from __future__ import annotations

import csv
from io import StringIO

import pandas as pd
import requests

from etf_extractors.base import BaseETFExtractor
from etf_extractors.utils import fetch_html, find_csv_link, HEADERS


class KraneSharesExtractor(BaseETFExtractor):
    @property
    def source_name(self) -> str:
        return "kraneshares"

    @property
    def fund_code(self) -> str:
        return "kmlm"

    def extract(self) -> pd.DataFrame:
        html = fetch_html(self.url)
        csv_url = find_csv_link(self.url, html)

        if not csv_url:
            raise RuntimeError("Could not find CSV link on KraneShares page.")

        response = requests.get(csv_url, headers=HEADERS, timeout=30)
        response.raise_for_status()
        text = response.content.decode("utf-8", errors="replace")

        lines = [line for line in text.splitlines() if line.strip()]
        parsed_rows = [next(csv.reader([line])) for line in lines]

        if len(parsed_rows) < 3:
            raise RuntimeError("KMLM CSV does not contain enough rows.")

        # Row 0 often contains title/as-of text merged with other fields
        # Row 1 is the real data header in the current KMLM CSV shape
        first_row = [str(x).strip() for x in parsed_rows[0]]
        headers = [str(x).strip() for x in parsed_rows[1]]

        expected_len = len(headers)
        data_rows = []

        for row in parsed_rows[2:]:
            if not row:
                continue

            if len(row) < expected_len:
                row = row + [""] * (expected_len - len(row))
            elif len(row) > expected_len:
                row = row[:expected_len]

            if all(str(x).strip() == "" for x in row):
                continue

            data_rows.append(row)

        df = pd.DataFrame(data_rows, columns=headers)

        # Normalize actual columns currently observed in your KMLM file
        column_map = {
            "Shares Held": "shares",
            "Market Value($)": "market_value",
            "Notional Value($)": "notional_value",
        }
        df = df.rename(columns=column_map).copy()

        # Extract as_of_date from row 0, e.g. "As of 2026-04-02"
        as_of_date = None
        for item in first_row:
            item = str(item).strip()
            if item.lower().startswith("as of "):
                as_of_date = item.replace("As of ", "").strip()
                break

        df["source"] = self.source_name
        df["fund"] = self.fund_code.upper()
        df["as_of_date"] = as_of_date

        for col in ["shares", "market_value", "notional_value"]:
            if col in df.columns:
                df[col] = (
                    df[col]
                    .astype(str)
                    .str.replace(",", "", regex=False)
                    .replace({"": None, "nan": None, "–": None, "-": None})
                )
                df[col] = pd.to_numeric(df[col], errors="coerce")

        # Unified final schema
        # KMLM current CSV does not reliably expose name/ticker/identifier in your current parsed version,
        # so fill them with None for schema consistency.
        final_df = pd.DataFrame({
            "source": df["source"],
            "fund": df["fund"],
            "as_of_date": df["as_of_date"],
            "name": None,
            "ticker": None,
            "identifier": None,
            "weight_pct": None,
            "shares": df["shares"] if "shares" in df.columns else None,
            "market_value": df["market_value"] if "market_value" in df.columns else None,
        })

        return final_df