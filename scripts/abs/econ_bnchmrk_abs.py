#!/usr/bin/env python3
"""
ABS county × NAICS2 benchmarking extract.

Mirrors notebooks/abs/econ_bnchmrk_2022_abs.ipynb:
  * Pulls 2-digit NAICS ABS data from the Census API.
  * Normalizes columns, converts PAYANN/RCPPDEMP from $1k → USD.
  * Derives total receipts (abs_rcpt_usd_amt = receipts per employee × employment).
  * Writes the tidy CSV locally and optionally to GCS.
"""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Optional

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
    url = f"https://api.census.gov/data/{year}/abscs"
    params = {"get": ABS_FIELDS, "for": "county:*", "INDLEVEL": "2"}
    resp = requests.get(url, params=params, timeout=60)
    resp.raise_for_status()
    header, *rows = resp.json()
    df = pd.DataFrame(rows, columns=header)
    df["year_num"] = year
    return df


def normalize_abs(df: pd.DataFrame) -> pd.DataFrame:
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
            "RCPPDEMP": "abs_rcpt_per_emp_usd_amt",
            "state": "state_fips_cd",
            "county": "cnty_fips_cd",
        }
    )

    usd_factor = 1000.0
    df["abs_payroll_usd_amt"] = df["abs_payroll_usd_amt"] * usd_factor
    df["abs_rcpt_per_emp_usd_amt"] = df["abs_rcpt_per_emp_usd_amt"] * usd_factor
    df["abs_rcpt_usd_amt"] = df["abs_rcpt_per_emp_usd_amt"] * df["abs_emp_num"]

    df["state_fips_cd"] = df["state_fips_cd"].str.zfill(2)
    df["cnty_fips_cd"] = df["cnty_fips_cd"].str.zfill(3)
    df["state_cnty_fips_cd"] = df["state_fips_cd"] + df["cnty_fips_cd"]

    columns = [
        "year_num",
        "state_cnty_fips_cd",
        "naics2_sector_cd",
        "cnty_nm",
        "geo_id",
        "naics2_sector_desc",
        "ind_level_num",
        "state_fips_cd",
        "cnty_fips_cd",
        "abs_firm_num",
        "abs_emp_num",
        "abs_payroll_usd_amt",
        "abs_rcpt_usd_amt",
        "abs_rcpt_per_emp_usd_amt",
    ]
    df = df[columns].sort_values(
        ["year_num", "state_cnty_fips_cd", "naics2_sector_cd"]
    )
    return df


def write_outputs(df: pd.DataFrame, out_csv: Path, gcs_uri: Optional[str]) -> None:
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
    parser.add_argument(
        "--gcs_uri",
        default=None,
        help="Optional gs:// URI to write the CSV (requires GCS credentials)",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    raw = fetch_abs(args.year)
    df = normalize_abs(raw)
    write_outputs(df, Path(args.out_csv), args.gcs_uri)


if __name__ == "__main__":
    main()
