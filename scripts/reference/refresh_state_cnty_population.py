#!/usr/bin/env python3
"""
Augment ref_state_cnty_uscb with county population.

Pulls county-level population from the Census ACS 5-year dataset (B01001_001E)
for a specified year, joins it onto the existing reference, and writes the
enriched CSV (including `population_num` and `population_year`).
"""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Tuple

import pandas as pd
import requests

ACS_TABLE_VAR = "B01001_001E"
ACS_DATASET = "acs/acs5"


def fetch_population(year: int) -> pd.DataFrame:
    """Fetch population counts for every county from the ACS API."""
    endpoint = f"https://api.census.gov/data/{year}/{ACS_DATASET}"
    params = {"get": f"NAME,{ACS_TABLE_VAR}", "for": "county:*", "in": "state:*"}
    resp = requests.get(endpoint, params=params, timeout=60)
    resp.raise_for_status()
    header, *rows = resp.json()
    df = pd.DataFrame(rows, columns=header)
    df["state_cnty_fips_cd"] = (
        df["state"].astype(str).str.zfill(2) + df["county"].astype(str).str.zfill(3)
    )
    df["population_num"] = pd.to_numeric(df[ACS_TABLE_VAR], errors="coerce").astype("Int64")
    df["population_year"] = year
    return df[["state_cnty_fips_cd", "population_num", "population_year"]]


def merge_population(
    ref_df: pd.DataFrame, pop_df: pd.DataFrame
) -> Tuple[pd.DataFrame, int]:
    """Join population columns onto reference table."""
    merged = ref_df.merge(pop_df, on="state_cnty_fips_cd", how="left")
    matched = merged.get("population_num", pd.Series(dtype="Int64")).notna().sum()
    return merged, matched


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Attach ACS population to ref_state_cnty_uscb."
    )
    parser.add_argument(
        "--ref_csv",
        default="data_clean/reference/ref_state_cnty_uscb.csv",
        help="Existing reference CSV to enrich (default: %(default)s)",
    )
    parser.add_argument(
        "--out_csv",
        default=None,
        help="Destination CSV (default: overwrite ref_csv)",
    )
    parser.add_argument(
        "--year",
        type=int,
        default=2022,
        help="ACS vintage to use for population (default: %(default)s)",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    ref_path = Path(args.ref_csv)
    out_path = Path(args.out_csv) if args.out_csv else ref_path

    if not ref_path.exists():
        raise FileNotFoundError(f"Reference CSV not found: {ref_path}")

    print(f"[POP] Loading reference: {ref_path}")
    ref_df = pd.read_csv(ref_path, dtype={"state_cnty_fips_cd": str})

    print(f"[POP] Fetching ACS population for {args.year} …")
    pop_df = fetch_population(args.year)

    merged, matched = merge_population(ref_df, pop_df)

    for col in ["land_area_num", "water_area_num", "population_num", "population_year"]:
        if col in merged.columns:
            merged[col] = pd.to_numeric(merged[col], errors="coerce").astype("Int64")
    print(f"[POP] Matched population for {matched:,} counties.")

    out_path.parent.mkdir(parents=True, exist_ok=True)
    merged.to_csv(out_path, index=False)
    print(f"[POP] Wrote updated reference: {out_path}")


if __name__ == "__main__":
    main()
