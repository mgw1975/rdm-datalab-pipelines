#!/usr/bin/env python3
"""
Clean BEA CAGDP2 (county GDP by industry) into a tidy CSV ready for loading.

Usage:
    python scripts/bea/gdp_bea.py \
        --bea_raw data_raw/bea/CAGDP2__ALL_AREAS_2001_2023.csv \
        --out data_clean/abs/gdp_bea.csv
"""

import argparse
from pathlib import Path
from typing import List

import pandas as pd

GDP_COL_TEMPLATE = "{year}_gdp_num"
SUPPRESSION_TOKENS = {"(D)", "(NA)"}


def load_bea_csv(csv_path: Path) -> pd.DataFrame:
    """Load the raw BEA CAGDP2 extract."""
    return pd.read_csv(csv_path, low_memory=False, encoding="latin1")


def tidy_bea(df: pd.DataFrame, years: List[int]) -> pd.DataFrame:
    """Select, rename, and clean GDP columns for the requested years."""
    cols = ["GeoFIPS", "LineCode", "IndustryClassification", "Description"]
    year_cols = [str(y) for y in years]
    missing = [c for c in cols + year_cols if c not in df.columns]
    if missing:
        raise ValueError(f"BEA file missing expected columns: {missing}")

    tidy = df.loc[:, cols + year_cols].copy()
    tidy = tidy.rename(
        columns={
            "GeoFIPS": "state_county_fips_cd",
            "LineCode": "line_cd",
            "IndustryClassification": "naics_sector_cd",
            "Description": "naics_sector_desc",
        }
    )
    tidy["state_county_fips_cd"] = (
        tidy["state_county_fips_cd"].astype(str).str.replace('"', "").str.strip()
    )
    tidy["state_county_fips_cd"] = tidy["state_county_fips_cd"].str.zfill(5)
    tidy = tidy[tidy["state_county_fips_cd"].str.len() == 5]
    tidy = tidy[tidy["state_county_fips_cd"] != "00000"].copy()

    for year in years:
        raw_col = str(year)
        clean_col = GDP_COL_TEMPLATE.format(year=year)
        tidy[clean_col] = pd.to_numeric(tidy[raw_col], errors="coerce") * 1000.0
        tidy = tidy.drop(columns=[raw_col])

    # Drop rows where every GDP column is NaN after coercion (fully suppressed rows)
    gdp_cols = [GDP_COL_TEMPLATE.format(year=y) for y in years]
    tidy = tidy.dropna(subset=gdp_cols, how="all")

    return tidy


def run_quality_checks(df: pd.DataFrame, years: List[int]) -> None:
    """Prototype QA checks for GDP outputs."""
    issues = []
    key = ["state_county_fips_cd", "line_cd", "naics_sector_cd"]
    dup_rows = df[df.duplicated(subset=key, keep=False)]
    if not dup_rows.empty:
        issues.append(
            f"Found {dup_rows.shape[0]} duplicate rows for key {key}; review deduping logic."
        )

    for year in years:
        col = GDP_COL_TEMPLATE.format(year=year)
        if (df[col].dropna() < 0).any():
            issues.append(f"Negative GDP detected in column {col}.")

    if issues:
        joined = "\n - ".join(issues)
        raise AssertionError(f"Data quality checks failed:\n - {joined}")


def count_suppressed_tokens(df: pd.DataFrame, years: List[int]) -> int:
    """Count suppressed tokens prior to numeric conversion."""
    total = 0
    for year in years:
        col = str(year)
        total += df[col].astype(str).isin(SUPPRESSION_TOKENS).sum()
    return total


def main() -> None:
    ap = argparse.ArgumentParser(description="Clean BEA CAGDP2 county GDP file.")
    ap.add_argument("--bea_raw", required=True, help="Path to raw BEA CAGDP2 CSV.")
    ap.add_argument(
        "--years",
        nargs="+",
        type=int,
        default=[2022, 2021, 2020],
        help="GDP years to retain (default: 2022 2021 2020).",
    )
    ap.add_argument(
        "--out",
        required=True,
        help="Output CSV path for the cleaned GDP table.",
    )
    args = ap.parse_args()

    raw_path = Path(args.bea_raw)
    out_path = Path(args.out)

    df_raw = load_bea_csv(raw_path)
    suppressed = count_suppressed_tokens(df_raw, args.years)
    tidy = tidy_bea(df_raw, args.years)
    run_quality_checks(tidy, args.years)

    out_path.parent.mkdir(parents=True, exist_ok=True)
    tidy.to_csv(out_path, index=False)
    print(
        f"Wrote {out_path} with {len(tidy):,} rows "
        f"(suppressed tokens encountered: {suppressed:,})."
    )


if __name__ == "__main__":
    main()
