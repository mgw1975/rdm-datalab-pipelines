#!/usr/bin/env python3
"""
Prepare the NAICS 2-digit reference table for BigQuery.

Reads the raw Census-provided sector file, normalizes column names, adds the
synthetic "00" (Total) and "99" (Unclassified) sectors, and writes the tidy CSV
used by `ref_naics2_uscb`.
"""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Optional

import pandas as pd

RAW_DEFAULT = Path("data_raw/naics/naics_2022_sector_2digit.csv")
OUT_DEFAULT = Path("data_clean/reference/ref_naics2_uscb.csv")


def load_raw(path: Path) -> pd.DataFrame:
    """Load the raw NAICS sector CSV (no header) into a DataFrame."""
    df = pd.read_csv(path, names=["naics2_sector_cd", "naics2_sector_desc"])
    df["naics2_sector_cd"] = df["naics2_sector_cd"].astype(str).str.strip()
    df["naics2_sector_desc"] = df["naics2_sector_desc"].astype(str).str.strip()
    return df


def append_extras(df: pd.DataFrame) -> pd.DataFrame:
    """Append the synthetic total/unclassified sectors."""
    extras = pd.DataFrame(
        [
            {"naics2_sector_cd": "00", "naics2_sector_desc": "Total for all sectors"},
            {"naics2_sector_cd": "99", "naics2_sector_desc": "Unclassified (suppression bucket)"},
        ]
    )
    df = pd.concat([df, extras], ignore_index=True)
    df = df.sort_values("naics2_sector_cd").reset_index(drop=True)
    return df


def write_output(df: pd.DataFrame, out_path: Path) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(out_path, index=False)
    print(f"Wrote {out_path} ({len(df)} rows).")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Prepare NAICS2 reference CSV.")
    parser.add_argument("--in_csv", default=str(RAW_DEFAULT), help="Raw NAICS sector CSV path.")
    parser.add_argument("--out_csv", default=str(OUT_DEFAULT), help="Destination CSV path.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    df = load_raw(Path(args.in_csv))
    df = append_extras(df)
    write_output(df, Path(args.out_csv))


if __name__ == "__main__":
    main()
