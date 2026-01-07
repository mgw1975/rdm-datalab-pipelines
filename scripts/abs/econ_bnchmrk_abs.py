#!/usr/bin/env python3
"""
ABS county × NAICS2 benchmarking extract.

Usage:
  python scripts/abs/econ_bnchmrk_abs.py \
      --years 2022 2023 \
      --out_csv data_clean/abs/econ_bnchmrk_abs_multiyear.csv
  # Manual step: upload econ_bnchmrk_abs_multiyear.csv to GCS after inspecting output

Mirrors notebooks/abs/econ_bnchmrk_2022_abs.ipynb:
  * Pulls 2-digit NAICS ABS data from the Census API.
  * Normalizes columns, converts PAYANN/RCPPDEMP from $1k → USD.
  * Derives total receipts (abs_rcpt_usd_amt = receipts per employee × employment).
  * Writes the tidy CSV locally (GCS upload handled manually downstream).

MVP Scope:
  * County-level ABS data is only available beginning in 2022. The default run
    produces a post-pandemic panel for 2022–2023. Additional years ≥2022 can be
    added later as the Census releases them.
"""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd
import requests

ABS_BASE_FIELDS = [
    "NAME",
    "GEO_ID",
    "FIRMPDEMP",
    "EMP",
    "PAYANN",
    "RCPPDEMP",
    "INDLEVEL",
]

ABS_YEAR_CONFIG = {
    2022: {"naics_field": "NAICS2022", "naics_label_field": "NAICS2022_LABEL"},
    2023: {"naics_field": "NAICS2022", "naics_label_field": "NAICS2022_LABEL"},
}

DEFAULT_STACKED_OUT = Path("data_clean/abs/econ_bnchmrk_abs_multiyear.csv")
DEFAULT_PER_YEAR_PATTERN = "data_clean/abs/econ_bnchmrk_abs_{year}.csv"
MVP_YEARS = [2022, 2023]
ABS_MIN_YEAR = 2022  # county-level ABS API coverage begins here


def year_config(year: int) -> dict:
    """Return the ABS metadata for a given year, defaulting to latest schema."""
    if year < ABS_MIN_YEAR:
        raise ValueError(
            f"County-level ABS API is only available for {ABS_MIN_YEAR}+; requested {year}."
        )
    if year in ABS_YEAR_CONFIG:
        return ABS_YEAR_CONFIG[year]
    # Fall back to most recent definition if a new year is requested
    latest_year = max(ABS_YEAR_CONFIG)
    cfg = ABS_YEAR_CONFIG[latest_year].copy()
    cfg["naics_field"] = cfg["naics_field"].replace(str(latest_year), str(year))
    cfg["naics_label_field"] = cfg["naics_label_field"].replace(str(latest_year), str(year))
    return cfg


def build_field_list(year: int) -> str:
    cfg = year_config(year)
    fields = ABS_BASE_FIELDS + [cfg["naics_field"], cfg["naics_label_field"]]
    return ",".join(fields)


def fetch_abs(year: int) -> pd.DataFrame:
    """Hit the Census ABS API for the requested year and return a DataFrame."""
    url = f"https://api.census.gov/data/{year}/abscs"
    params = {"get": build_field_list(year), "for": "county:*", "INDLEVEL": "2"}
    print(f"[ABS] Fetching year {year} from {url} …")
    resp = requests.get(url, params=params, timeout=60)
    resp.raise_for_status()
    header, *rows = resp.json()
    df = pd.DataFrame(rows, columns=header)
    df["year_num"] = year
    return df


def filter_abs_private_employer(df: pd.DataFrame, year: int) -> pd.DataFrame:
    """
    Align raw ABS universe with QCEW by keeping employer firms (FIRMPDEMP > 0)
    and dropping Public Administration (NAICS 92). NAICS 99 remains.
    """
    filtered = df.copy()
    filtered["FIRMPDEMP"] = pd.to_numeric(filtered["FIRMPDEMP"], errors="coerce")
    before = len(filtered)
    filtered = filtered[filtered["FIRMPDEMP"] > 0]

    cfg = year_config(year)
    naics_field = cfg["naics_field"]
    filtered[naics_field] = filtered[naics_field].astype(str).str.strip()
    filtered = filtered[filtered[naics_field] != "92"]
    after = len(filtered)
    print(
        f"[ABS] Filtered employer-only & private sectors for {year}: "
        f"{before:,} → {after:,} rows."
    )
    return filtered


def normalize_abs(df: pd.DataFrame, year: int) -> pd.DataFrame:
    """Standardize column names, convert currency units, and derive totals."""
    print("[ABS] Casting numerics and standardizing column names …")
    numeric_cols = ["FIRMPDEMP", "EMP", "PAYANN", "RCPPDEMP"]
    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    df["year_num"] = pd.to_numeric(df["year_num"], errors="coerce")

    cfg = year_config(year)
    naics_field = cfg["naics_field"]
    naics_label_field = cfg["naics_label_field"]

    df = df.rename(
        columns={
            "NAME": "cnty_nm",
            "GEO_ID": "geo_id",
            naics_field: "naics2_sector_cd",
            naics_label_field: "naics2_sector_desc",
            "INDLEVEL": "ind_level_num",
            "FIRMPDEMP": "abs_firm_num",
            "EMP": "abs_emp_num",
            "PAYANN": "abs_payroll_usd_amt",
            "RCPPDEMP": "abs_rcpt_usd_amt",
            "state": "state_fips_cd",
            "county": "cnty_fips_cd",
        }
    )
    df = df.loc[:, ~df.columns.duplicated()].copy()

    # PAYANN/RCPPDEMP come from the API in $1,000s. Convert to dollars and derive
    # receipts-per-employee from the totals.
    df["abs_payroll_usd_amt"] = df["abs_payroll_usd_amt"] * 1000.0
    df["abs_rcpt_usd_amt"] = df["abs_rcpt_usd_amt"] * 1000.0

    emp_denom = df["abs_emp_num"].replace({0: np.nan})
    firm_denom = df["abs_firm_num"].replace({0: np.nan})

    per_emp = (df["abs_rcpt_usd_amt"] / emp_denom).round(6)
    df["abs_rcpt_per_emp_usd_amt"] = per_emp.astype(float)

    per_firm = (df["abs_rcpt_usd_amt"] / firm_denom).round(6)
    df["abs_rcpt_per_firm_usd_amt"] = per_firm.astype(float)

    # Build the canonical 5-digit FIPS string (state + county).
    df["state_fips_cd"] = df["state_fips_cd"].str.zfill(2)
    df["cnty_fips_cd"] = df["cnty_fips_cd"].str.zfill(3)
    df["state_cnty_fips_cd"] = df["state_fips_cd"] + df["cnty_fips_cd"]
    print("[ABS] Finished normalization + FIPS assembly.")
    columns = [
        "year_num",
        "state_cnty_fips_cd",
        "naics2_sector_cd",
        "cnty_nm",
        "geo_id",
        "naics2_sector_desc",
        "ind_level_num",
        "abs_firm_num",
        "abs_emp_num",
        "abs_payroll_usd_amt",
        "abs_rcpt_usd_amt",
        "abs_rcpt_per_emp_usd_amt",
        "abs_rcpt_per_firm_usd_amt",
    ]
    df = df[columns].sort_values(
        ["year_num", "state_cnty_fips_cd", "naics2_sector_cd"]
    )
    return df


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Download ABS county × NAICS2 benchmarking data."
    )
    parser.add_argument(
        "--year",
        type=int,
        default=None,
        help="Optional single ABS vintage (default: use MVP years)",
    )
    parser.add_argument(
        "--years",
        type=int,
        nargs="+",
        help="Optional list of ABS vintages to process (default: MVP years)",
    )
    parser.add_argument(
        "--per_year_pattern",
        default=DEFAULT_PER_YEAR_PATTERN,
        help="Pattern for per-year outputs (use '{year}' placeholder)",
    )
    parser.add_argument(
        "--out_csv",
        default=str(DEFAULT_STACKED_OUT),
        help=f"Combined multiyear output path (default: {DEFAULT_STACKED_OUT})",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if args.years:
        years = sorted(set(args.years))
    elif args.year:
        years = [args.year]
    else:
        years = MVP_YEARS.copy()

    per_year_template = args.per_year_pattern
    stacked_frames = []

    for year in years:
        raw = fetch_abs(year)
        raw = filter_abs_private_employer(raw, year)
        df = normalize_abs(raw, year)
        per_year_path = Path(per_year_template.format(year=year))
        per_year_path.parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(per_year_path, index=False)
        print(f"[ABS] Wrote {per_year_path} ({len(df):,} rows).")
        stacked_frames.append(df)

    if args.out_csv:
        combined = pd.concat(stacked_frames, ignore_index=True)
        dupes = combined.duplicated(subset=["year_num", "state_cnty_fips_cd", "naics2_sector_cd"]).sum()
        if dupes:
            raise AssertionError(f"Found {dupes} duplicate rows in combined ABS output.")
        out_path = Path(args.out_csv)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        combined.to_csv(out_path, index=False)
        print(f"[ABS] Wrote combined dataset: {out_path} ({len(combined):,} rows).")


if __name__ == "__main__":
    main()
