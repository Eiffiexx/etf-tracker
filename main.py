from __future__ import annotations

import argparse
import sys

from etf_extractors.blueprint import BlueprintExtractor
from etf_extractors.imgp import IMGPExtractor
from etf_extractors.kraneshares import KraneSharesExtractor


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="ETF website extractor")
    parser.add_argument("--source", required=True, choices=["kraneshares", "blueprint", "imgp"])
    parser.add_argument("--url", required=True)
    parser.add_argument("--format", default="csv", choices=["csv", "json"])
    return parser


def get_extractor(source: str, url: str):
    if source == "kraneshares":
        return KraneSharesExtractor(url=url)
    if source == "blueprint":
        return BlueprintExtractor(url=url)
    if source == "imgp":
        return IMGPExtractor(url=url)
    raise ValueError(f"Unsupported source: {source}")


def main():
    parser = build_parser()
    args = parser.parse_args()

    try:
        extractor = get_extractor(args.source, args.url)
        df = extractor.extract()

        print("\nPreview:")
        print(df.head(10).to_string(index=False))

        output_path = extractor.save(df, fmt=args.format)
        print(f"\nSaved to: {output_path}")

    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()