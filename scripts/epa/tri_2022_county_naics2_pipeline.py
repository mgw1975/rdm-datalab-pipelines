#!/usr/bin/env python3
"""
TRI 2022 → county × NAICS2 aggregation (with optional merge into RDM portfolio).

Usage examples:
---------------
# Minimal: aggregate TRI facilities → county×NAICS2 for California
python tri_2022_county_naics2_pipeline.py \
  --tri_csv tri_facilities_2022.csv \
  --state_filter 06 \
  --out_tri tri_2022_county_naics2_CA.csv

# Aggregate for multiple states and also merge with ABS + QCEW
python tri_2022_county_naics2_pipeline.py \
  --tri_csv tri_facilities_2022.csv \
  --state_filter 06 41 32 \
  --abs_csv abs_ca_county_naics2.csv \
  --qcew_csv qcew_ca_county_naics2_2022.csv \
  --out_tri tri_2022_county_naics2_multi.csv \
  --out_merged portfolio_abs_qcew_tri_county_naics2.csv

Notes:
------
- TRI releases are in **pounds** (lbs).
- Keys are standardized to zero-padded strings: state (2), county (3), naics2 (2).
- ABS payroll/receipts are often provided in $1,000s; convert to dollars before merging.
- QCEW wages are in dollars; employment is establishment-based.
"""

import argparse
import pandas as pd
import numpy as np
from typing import List, Optional

def zfill_series(s: pd.Series, n: int) -> pd.Series:
    return s.astype(str).str.extract(r"(\d+)", expand=False).fillna("").str.zfill(n)

def load_tri_facilities(csv_path: str) -> pd.DataFrame:
    # Expected columns (exact names vary slightly by release schema)
    # Assumed present (case-sensitive): FIPS_STATE, FIPS_COUNTY, PRIMARY_NAICS, TOTAL_RELEASE_LBS or TOTAL_RELEASES_LBS
    tri = pd.read_csv(csv_path, dtype=str, low_memory=False)
    # Normalize release column name(s)
    if "TOTAL_RELEASES_LBS" in tri.columns:
        tri["TOTAL_RELEASES_LBS"] = pd.to_numeric(tri["TOTAL_RELEASES_LBS"], errors="coerce")
        tri.rename(columns={"TOTAL_RELEASES_LBS":"tri_releases_lbs"}, inplace=True)
    elif "TOTAL_RELEASE_LBS" in tri.columns:
        tri["TOTAL_RELEASE_LBS"] = pd.to_numeric(tri["TOTAL_RELEASE_LBS"], errors="coerce")
        tri.rename(columns={"TOTAL_RELEASE_LBS":"tri_releases_lbs"}, inplace=True)
    else:
        # Try to build total from commonly present media fields
        release_cols = [c for c in tri.columns if c.upper().endswith("_RELEASE_LBS")]
        if not release_cols:
            raise ValueError("Could not locate TRI release columns. Expected TOTAL_RELEASES_LBS or *_RELEASE_LBS fields.")
        tri[release_cols] = tri[release_cols].apply(pd.to_numeric, errors="coerce").fillna(0.0)
        tri["tri_releases_lbs"] = tri[release_cols].sum(axis=1)

    # Standardize keys
    for col, width in [("FIPS_STATE", 2), ("FIPS_COUNTY", 3)]:
        if col not in tri.columns:
            raise ValueError(f"Expected column '{col}' not found in TRI file.")
        tri[col] = zfill_series(tri[col], width)

    # NAICS: accept PRIMARY_NAICS or PRIMARY_NAICS_CODE
    naics_col = None
    for candidate in ["PRIMARY_NAICS", "PRIMARY_NAICS_CODE", "NAICS_CODE"]:
        if candidate in tri.columns:
            naics_col = candidate
            break
    if naics_col is None:
        raise ValueError("Could not find PRIMARY_NAICS / PRIMARY_NAICS_CODE / NAICS_CODE column in TRI file.")
    tri["naics2"] = tri[naics_col].astype(str).str[:2]

    # Canonical key names
    tri["state"]  = tri["FIPS_STATE"]
    tri["county"] = tri["FIPS_COUNTY"]

    # Keep minimal columns
    keep = ["state","county","naics2","tri_releases_lbs"]
    return tri[keep]

def aggregate_tri(tri_df: pd.DataFrame, state_filter: Optional[List[str]] = None) -> pd.DataFrame:
    df = tri_df.copy()
    if state_filter:
        state_filter = [str(s).zfill(2) for s in state_filter]
        df = df[df["state"].isin(state_filter)]
    grouped = (df.groupby(["state","county","naics2"], as_index=False)["tri_releases_lbs"]
                 .sum())
    # Non-negativity check
    assert (grouped["tri_releases_lbs"].dropna() >= 0).all()
    return grouped

def load_abs(abs_csv: str) -> pd.DataFrame:
    abs_df = pd.read_csv(abs_csv, dtype=str)
    # Expected columns include: state, county, NAICS2022, FIRMPDEMP, EMP, PAYANN, RCPPDEMP
    abs_df["state"]  = zfill_series(abs_df.get("state") if "state" in abs_df.columns else abs_df.get("STATEFP"), 2)
    abs_df["county"] = zfill_series(abs_df.get("county") if "county" in abs_df.columns else abs_df.get("COUNTYFP"), 3)
    abs_df["naics2"] = abs_df["NAICS2022"].astype(str).str[:2]

    # Convert $1,000 → $
    for k in ["PAYANN", "RCPPDEMP"]:
        if k in abs_df.columns:
            abs_df[k] = pd.to_numeric(abs_df[k], errors="coerce") * 1000

    rename_map = {
        "FIRMPDEMP":"abs_firms",
        "EMP":"abs_emp",
        "PAYANN":"abs_payroll_usd",
        "RCPPDEMP":"abs_receipts_usd",
    }
    abs_df = abs_df.rename(columns=rename_map)
    for c in ["abs_firms","abs_emp","abs_payroll_usd","abs_receipts_usd"]:
        if c in abs_df.columns:
            abs_df[c] = pd.to_numeric(abs_df[c], errors="coerce")
    return abs_df[["state","county","naics2","abs_firms","abs_emp","abs_payroll_usd","abs_receipts_usd"]].drop_duplicates()

def load_qcew(qcew_csv: str) -> pd.DataFrame:
    q = pd.read_csv(qcew_csv, dtype=str)
    # Expected raw columns: state_fips, county_fips, naics, annual_avg_emplvl, total_annual_wages, avg_weekly_wage, year
    q["state"]  = zfill_series(q.get("state_fips"), 2)
    q["county"] = zfill_series(q.get("county_fips"), 3)
    q["naics2"] = q["naics"].astype(str).str[:2]
    q = q.rename(columns={
        "annual_avg_emplvl":"qcew_emp",
        "total_annual_wages":"qcew_wages_usd",
        "avg_weekly_wage":"qcew_avg_weekly_wage_usd"
    })
    for c in ["qcew_emp","qcew_wages_usd","qcew_avg_weekly_wage_usd"]:
        q[c] = pd.to_numeric(q[c], errors="coerce")
    return q[["state","county","naics2","qcew_emp","qcew_wages_usd","qcew_avg_weekly_wage_usd","year"]].drop_duplicates()

def merge_portfolio(abs_df: Optional[pd.DataFrame], qcew_df: Optional[pd.DataFrame], tri_g: pd.DataFrame) -> pd.DataFrame:
    key = ["state","county","naics2"]
    merged = tri_g.copy()
    if abs_df is not None:
        merged = merged.merge(abs_df, on=key, how="left")
    if qcew_df is not None:
        merged = merged.merge(qcew_df, on=key, how="left")

    # Derived intensity metrics (guard against div-by-zero)
    if "qcew_emp" in merged.columns:
        merged["tri_lbs_per_emp"]  = np.where(merged["qcew_emp"].fillna(0) > 0,
                                              merged["tri_releases_lbs"] / merged["qcew_emp"], np.nan)
    if "abs_firms" in merged.columns:
        merged["tri_lbs_per_firm"] = np.where(merged["abs_firms"].fillna(0) > 0,
                                              merged["tri_releases_lbs"] / merged["abs_firms"], np.nan)

    # Simple QA
    assert merged.duplicated(subset=key).sum() == 0
    assert (merged["tri_releases_lbs"].dropna() >= 0).all()

    return merged

def main():
    parser = argparse.ArgumentParser(description="EPA TRI 2022 aggregation to county × NAICS2, with optional ABS/QCEW merge.")
    parser.add_argument("--tri_csv", required=True, help="Path to TRI facilities CSV for 2022")
    parser.add_argument("--state_filter", nargs="*", default=["06"], help="Optional list of state FIPS (zero-padded) to include (default: 06 for CA)")
    parser.add_argument("--out_tri", required=True, help="Output CSV path for county×NAICS2 TRI aggregation")
    parser.add_argument("--abs_csv", default=None, help="Optional ABS CSV at county×NAICS2 for merge")
    parser.add_argument("--qcew_csv", default=None, help="Optional QCEW CSV at county×NAICS2 for merge")
    parser.add_argument("--out_merged", default=None, help="Optional output CSV path for merged portfolio with TRI")
    args = parser.parse_args()

    tri_df = load_tri_facilities(args.tri_csv)
    tri_g = aggregate_tri(tri_df, state_filter=args.state_filter)
    tri_g.to_csv(args.out_tri, index=False)

    abs_df = load_abs(args.abs_csv) if args.abs_csv else None
    qcew_df = load_qcew(args.qcew_csv) if args.qcew_csv else None

    if args.out_merged:
        merged = merge_portfolio(abs_df, qcew_df, tri_g)
        merged.to_csv(args.out_merged, index=False)

if __name__ == "__main__":
    main()
