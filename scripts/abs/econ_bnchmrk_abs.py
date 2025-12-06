#!/usr/bin/env python3
"""
ABS county × NAICS2 benchmarking extract.

Usage:
  python scripts/abs/econ_bnchmrk_abs.py \
      --year 2022 \
      --out_csv data_clean/abs/econ_bnchmrk_abs.csv
  # Manual step: upload econ_bnchmrk_abs.csv to GCS after inspecting output

Mirrors notebooks/abs/econ_bnchmrk_2022_abs.ipynb:
  * Pulls 2-digit NAICS ABS data from the Census API.
  * Normalizes columns, converts PAYANN/RCPPDEMP from $1k → USD.
  * Derives total receipts (abs_rcpt_usd_amt = receipts per employee × employment).
  * Writes the tidy CSV locally (GCS upload handled manually downstream).
"""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd
import requests

ABS_FIELDS = ",".join(
    [
        "NAME",
        "GEO_ID",
        "NAICS2022",
        "NAICS2022_LABEL",
        "FIRMPDEMP",
        "EMP",
        "PAYANN",
        "RCPPDEMP",
        "INDLEVEL",
    ]
)

DEFAULT_OUT = Path("data_clean/abs/econ_bnchmrk_abs.csv")


def fetch_abs(year: int) -> pd.DataFrame:
    """Hit the Census ABS API for the requested year and return a DataFrame."""
    url = f"https://api.census.gov/data/{year}/abscs"
    params = {"get": ABS_FIELDS, "for": "county:*", "INDLEVEL": "2"}
    print(f"[ABS] Fetching year {year} from {url} …")
    resp = requests.get(url, params=params, timeout=60)
    resp.raise_for_status()
    header, *rows = resp.json()
    df = pd.DataFrame(rows, columns=header)
    df["year_num"] = year
    return df


def normalize_abs(df: pd.DataFrame) -> pd.DataFrame:
    """Standardize column names, convert currency units, and derive totals."""
    print("[ABS] Casting numerics and standardizing column names …")
    numeric_cols = ["FIRMPDEMP", "EMP", "PAYANN", "RCPPDEMP"]
    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    df["year_num"] = pd.to_numeric(df["year_num"], errors="coerce")

    df = df.rename(
        columns={
            "NAME": "cnty_nm",
            "GEO_ID": "geo_id",
            "NAICS2022": "naics2_sector_cd",
            "NAICS2022_LABEL": "naics2_sector_desc",
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


def write_outputs(df: pd.DataFrame, out_csv: Path, gcs_uri: Optional[str]) -> None:
    """Persist the tidy frame locally and, optionally, to Cloud Storage."""
    out_csv.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(out_csv, index=False)
    print(f"Wrote {out_csv} ({len(df):,} rows).")

    if gcs_uri:
        try:
            df.to_csv(gcs_uri, index=False)
            print(f"Wrote GCS object: {gcs_uri}")
        except Exception as exc:  # pragma: no cover
            print(f"[WARN] Failed to write to {gcs_uri}: {exc}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Download ABS county × NAICS2 benchmarking data."
    )
    parser.add_argument("--year", type=int, default=2022, help="ABS vintage (default 2022)")
    parser.add_argument(
        "--out_csv",
        default=str(DEFAULT_OUT),
        help=f"Local CSV output path (default: {DEFAULT_OUT})",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    raw = fetch_abs(args.year)
    df = normalize_abs(raw)
    write_outputs(df, Path(args.out_csv), gcs_uri=None)


if __name__ == "__main__":
    main()
