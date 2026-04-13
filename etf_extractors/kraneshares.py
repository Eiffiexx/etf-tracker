from __future__ import annotations

import re
from pathlib import Path
from typing import Optional

import pandas as pd
import pdfplumber

from etf_extractors.base import BaseETFExtractor


class KraneSharesExtractor(BaseETFExtractor):
    def __init__(self, url: str, output_dir: str = "out", pdf_path: str = "kmlm.pdf"):
        super().__init__(url=url, output_dir=output_dir)
        self.pdf_path = Path(pdf_path)

    @property
    def source_name(self) -> str:
        return "kraneshares"

    @property
    def fund_code(self) -> str:
        return "kmlm"

    def extract(self) -> pd.DataFrame:
        if not self.pdf_path.exists():
            raise RuntimeError(
                f"KMLM PDF not found at: {self.pdf_path}. "
                "Place kmlm.pdf in the project root."
            )

        rows = self._extract_rows_from_pdf(self.pdf_path)

        if not rows:
            raise RuntimeError("Could not extract KMLM exposure rows from the PDF.")

        df = pd.DataFrame(rows)

        final_cols = [
            "source",
            "fund",
            "as_of_date",
            "section",
            "name",
            "identifier",
            "position",
            "weight_pct",
            "current_exposure",
        ]

        final_cols = [c for c in final_cols if c in df.columns]
        return df[final_cols]

    def _extract_rows_from_pdf(self, pdf_path: Path) -> list[dict]:
        with pdfplumber.open(str(pdf_path)) as pdf:
            all_text = "\n".join((page.extract_text() or "") for page in pdf.pages)

            as_of_date = self._extract_as_of_date(all_text)

            if len(pdf.pages) < 2:
                raise RuntimeError("KMLM PDF does not have a second page for exposures.")

            page2_text = pdf.pages[1].extract_text() or ""

        print("\n=== PAGE 2 RAW TEXT START ===")
        print(page2_text)
        print("=== PAGE 2 RAW TEXT END ===\n")

        lines = [self._clean_line(line) for line in page2_text.splitlines()]
        lines = [line for line in lines if line]

        rows: list[dict] = []
        state: Optional[str] = None

        for line in lines:
            if line.startswith("Commodity Exposures"):
                state = "dual"
                continue

            if state == "dual":
                if line.startswith("as of"):
                    continue

                if line.startswith("Fixed Income Exposures"):
                    state = "fixed_header"
                    continue

                matches = self._find_exposure_matches(line)

                if len(matches) >= 1:
                    rows.append(
                        self._build_exposure_row(
                            section="commodity",
                            as_of_date=as_of_date,
                            name=matches[0]["name"],
                            identifier=matches[0]["identifier"],
                            position=matches[0]["position"],
                            weight_pct=matches[0]["weight_pct"],
                        )
                    )

                if len(matches) >= 2:
                    rows.append(
                        self._build_exposure_row(
                            section="currency",
                            as_of_date=as_of_date,
                            name=matches[1]["name"],
                            identifier=matches[1]["identifier"],
                            position=matches[1]["position"],
                            weight_pct=matches[1]["weight_pct"],
                        )
                    )

                continue

            if state == "fixed_header":
                # The line immediately after "Fixed Income Exposures..." is still the last commodity row
                if line.startswith("as of"):
                    state = "fixed"
                    continue

                matches = self._find_exposure_matches(line)
                if len(matches) >= 1:
                    rows.append(
                        self._build_exposure_row(
                            section="commodity",
                            as_of_date=as_of_date,
                            name=matches[0]["name"],
                            identifier=matches[0]["identifier"],
                            position=matches[0]["position"],
                            weight_pct=matches[0]["weight_pct"],
                        )
                    )
                continue

            if state == "fixed":
                if line.startswith("as of"):
                    continue

                if line.startswith("Collateral and Currency Management"):
                    state = "collateral"
                    continue

                matches = self._find_exposure_matches(line)
                for match in matches:
                    rows.append(
                        self._build_exposure_row(
                            section="fixed_income",
                            as_of_date=as_of_date,
                            name=match["name"],
                            identifier=match["identifier"],
                            position=match["position"],
                            weight_pct=match["weight_pct"],
                        )
                    )
                continue

            if state == "collateral":
                if (
                    line.startswith("Cash ")
                    or line.startswith("B ")
                    or line.startswith("JAPANESE YEN ")
                    or line.startswith("EURO ")
                    or line.startswith("BRITISH STERLING POUND ")
                    or line.startswith("CANADIAN DOLLAR ")
                ):
                    parsed = self._parse_collateral_row(line, as_of_date)
                    if parsed is not None:
                        rows.append(parsed)
                continue

        return rows

    @staticmethod
    def _clean_line(line: str) -> str:
        return re.sub(r"\s+", " ", line).strip()

    @staticmethod
    def _extract_as_of_date(text: str) -> Optional[str]:
        patterns = [
            r"as of (\d{2}/\d{2}/\d{4})",
            r"As of (\d{2}/\d{2}/\d{4})",
            r"As of (\d{4}-\d{2}-\d{2})",
        ]
        for pattern in patterns:
            match = re.search(pattern, text)
            if match:
                return match.group(1)
        return None

    def _find_exposure_matches(self, line: str) -> list[dict]:
        """
        Finds one or two exposure rows inside a line, e.g.
        GOLD 100 OZ FUTR JUN26 GCM6 Long 7.61% AUDUSD CRNCY FUT MAR26 ADH6 Long 16.5%
        """

        pattern = re.compile(
            r"(?P<name>.+?)\s+"
            r"(?P<identifier>[A-Z0-9]{1,6}(?:\s[A-Z0-9]{1,4})?)\s+"
            r"(?P<position>Long|Short)\s+"
            r"(?P<weight>-?\d+(?:\.\d+)?)%"
        )

        matches = []
        for match in pattern.finditer(line):
            matches.append(
                {
                    "name": match.group("name").strip(),
                    "identifier": match.group("identifier").strip(),
                    "position": match.group("position").strip(),
                    "weight_pct": pd.to_numeric(match.group("weight"), errors="coerce"),
                }
            )

        return matches

    def _build_exposure_row(
        self,
        section: str,
        as_of_date: Optional[str],
        name: str,
        identifier: str,
        position: str,
        weight_pct: float,
    ) -> dict:
        return {
            "source": self.source_name,
            "fund": self.fund_code.upper(),
            "as_of_date": as_of_date,
            "section": section,
            "name": name,
            "identifier": identifier,
            "position": position,
            "weight_pct": weight_pct,
            "current_exposure": None,
        }

    def _parse_collateral_row(self, line: str, as_of_date: Optional[str]) -> Optional[dict]:
        """
        Parses lines like:
        Cash USD 69,311,625 69,311,625 35.64%
        B 03/31/26 US912797TB33 27,000,000 26,921,047 13.84%
        JAPANESE YEN JPY 215,797,145 1,381,853 0.71%
        """

        # Remove trailing weight
        weight_match = re.search(r"(-?\d+(?:\.\d+)?)%$", line)
        if not weight_match:
            return None

        weight_pct = pd.to_numeric(weight_match.group(1), errors="coerce")
        prefix = line[:weight_match.start()].strip()

        tokens = prefix.split()

        if len(tokens) < 3:
            return None

        # current exposure is the last numeric token before the percent
        current_exposure_token = tokens[-1].replace(",", "")
        current_exposure = pd.to_numeric(current_exposure_token, errors="coerce")

        # Remove current exposure token
        tokens = tokens[:-1]

        # Remove quantity/par value token if present
        if tokens and re.fullmatch(r"-?[\d,]+(?:\.\d+)?", tokens[-1]):
            tokens = tokens[:-1]

        if len(tokens) < 2:
            return None

        identifier = tokens[-1]
        name = " ".join(tokens[:-1]).strip()

        return {
            "source": self.source_name,
            "fund": self.fund_code.upper(),
            "as_of_date": as_of_date,
            "section": "collateral",
            "name": name,
            "identifier": identifier,
            "position": None,
            "weight_pct": weight_pct,
            "current_exposure": current_exposure,
        }