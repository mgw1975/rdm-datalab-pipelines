#!/usr/bin/env python3
"""
ABS 2022 Pull — CA counties × NAICS2 (firms, employment, payroll, receipts)
- Dataset: Census ABS 2022, /data/2022/abscs
- Grain: county × 2-digit NAICS (INDLEVEL=2) within a given state (default CA)
- Outputs:
    1) ABS_2022_CA_allcounties_NAICS2.csv        (long format)
    2) ABS22_CA_county_by_NAICS2_firms.csv       (wide pivot: FIRMPDEMP)
    3) ABS22_CA_county_by_NAICS2_emp.csv         (wide pivot: EMP)
    4) ABS22_CA_county_by_NAICS2_payroll_kUSD.csv(wide pivot: PAYANN in $1k)
    5) ABS22_CA_county_by_NAICS2_receipts_kUSD.csv(wide pivot: RCPPDEMP in $1k)
- Also derives row-level metrics and runs QA checks.

NOTE: PAYANN and RCPPDEMP are in thousands of dollars ($1,000s).
"""

from __future__ import annotations

import sys
import requests
import pandas as pd
import numpy as np


ABS_URL = "https://api.census.gov/data/2022/abscs"
GET_FIELDS = [
    "NAME","GEO_ID",
    "NAICS2022","NAICS2022_LABEL","INDLEVEL",
    "FIRMPDEMP","EMP","PAYANN","RCPPDEMP",
    "state","county"
]


def fetch_abs_abscs_county_naics2(state_fips: str = "06") -> pd.DataFrame:
    """
    Pull ABS 2022 ABScS for all counties in a state at NAICS2 (INDLEVEL=2).
    Returns a pandas DataFrame.
    """
    params = {
        "get": ",".join(GET_FIELDS),
        "for": "county:*",
        "in": f"state:{state_fips}",
        "INDLEVEL": "2"
    }
    r = requests.get(ABS_URL, params=params, timeout=90)
    r.raise_for_status()
    data = r.json()
    if not data or len(data) < 2:
        raise RuntimeError("Empty response from ABS API (check parameters or network).")
    hdr = data[0]
    df = pd.DataFrame(data[1:], columns=hdr)
    return df


def enforce_types(df: pd.DataFrame) -> pd.DataFrame:
    """Ensure numeric columns are numeric and FIPS are zero-padded."""
    out = df.copy()
    # FIPS
    out["state"] = out["state"].astype(str).str.zfill(2)
    out["county"] = out["county"].astype(str).str.zfill(3)

    # Numerics
    for c in ["FIRMPDEMP","EMP","PAYANN","RCPPDEMP"]:
        if c in out.columns:
            out[c] = pd.to_numeric(out[c], errors="coerce")

    return out


def add_derived_metrics(df: pd.DataFrame) -> pd.DataFrame:
    """
    Adds safe row-level derived metrics:
      - avg_wage_per_emp_dollars
      - receipts_per_firm_dollars
      - payroll_to_receipts (ratio)
      - receipts_per_emp_dollars
      - firms_per_1k_emp
    """
    out = df.copy()

    # Convert PAYANN/RCPPDEMP from $1k → $ for $-denom metrics
    pay_dollars = out["PAYANN"] * 1000.0
    rcpts_dollars = out["RCPPDEMP"] * 1000.0

    def sdiv(num, den):
        den = pd.to_numeric(den, errors="coerce")
        return np.where(den > 0, num / den, np.nan)

    out["avg_wage_per_emp_dollars"]  = sdiv(pay_dollars, out["EMP"])
    out["receipts_per_firm_dollars"] = sdiv(rcpts_dollars, out["FIRMPDEMP"])
    out["payroll_to_receipts"]       = sdiv(out["PAYANN"], out["RCPPDEMP"])  # both in $1k
    out["receipts_per_emp_dollars"]  = sdiv(rcpts_dollars, out["EMP"])
    out["firms_per_1k_emp"]          = sdiv(out["FIRMPDEMP"] * 1000.0, out["EMP"])

    return out


def run_quality_checks(df: pd.DataFrame) -> None:
    """Raise AssertionError if any QA check fails."""
    # 1) uniqueness of key at this grain
    dup_count = df.duplicated(subset=["state","county","NAICS2022"]).sum()
    assert dup_count == 0, f"Found {dup_count} duplicate (state, county, NAICS2022) rows."

    # 2) non-negatives for core measures where present
    for c in ["FIRMPDEMP","EMP","PAYANN","RCPPDEMP"]:
        if c in df.columns:
            nonneg = df[c].dropna().ge(0).all()
            assert nonneg, f"Negative values detected in {c}."

    # 3) INDLEVEL confirm
    if "INDLEVEL" in df.columns:
        assert (df["INDLEVEL"].astype(str) == "2").all(), "INDLEVEL not all '2' (2-digit NAICS) as expected."


def pivot_metric(df: pd.DataFrame, metric: str) -> pd.DataFrame:
    """Return a county×NAICS2 wide table for a given metric."""
    wide = (df.pivot_table(index=["NAME","state","county"],
                           columns="NAICS2022",
                           values=metric,
                           aggfunc="first")
              .sort_index())
    # ensure column order sorted by NAICS code
    wide = wide.reindex(sorted(wide.columns, key=lambda x: str(x)), axis=1)
    return wide


def main(state_fips: str = "06") -> int:
    print(f"Fetching ABS ABScS 2022 for state FIPS={state_fips} (all counties, NAICS2)...")
    df = fetch_abs_abscs_county_naics2(state_fips)
    print(f"Rows fetched: {len(df):,}")

    df = enforce_types(df)
    run_quality_checks(df)

    # Save long/baseline
    long_out = "ABS_2022_CA_allcounties_NAICS2.csv" if state_fips == "06" else f"ABS_2022_state{state_fips}_counties_NAICS2.csv"
    df.to_csv(long_out, index=False)
    print(f"Saved baseline long file: {long_out}")

    # Derived metrics
    dfm = add_derived_metrics(df)

    # Pivots
    pivots = {
        "FIRMPDEMP": "ABS22_CA_county_by_NAICS2_firms.csv",
        "EMP": "ABS22_CA_county_by_NAICS2_emp.csv",
        "PAYANN": "ABS22_CA_county_by_NAICS2_payroll_kUSD.csv",
        "RCPPDEMP": "ABS22_CA_county_by_NAICS2_receipts_kUSD.csv",
    }
    for metric, fname in pivots.items():
        wide = pivot_metric(dfm, metric)
        wide.to_csv(fname)
        print(f"Saved pivot for {metric}: {fname} (shape={wide.shape})")

    # Preview a few derived columns for sanity
    preview_cols = ["state","county","NAICS2022","FIRMPDEMP","EMP","PAYANN","RCPPDEMP",
                    "avg_wage_per_emp_dollars","receipts_per_firm_dollars","payroll_to_receipts"]
    print("\nPreview with derived metrics (head):")
    print(dfm[preview_cols].head(10).to_string(index=False))

    print("\nAll QA checks passed. Done.")
    return 0


if __name__ == "__main__":
    st = sys.argv[1] if len(sys.argv) > 1 else "06"  # default CA
    raise SystemExit(main(st))
