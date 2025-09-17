
# abs_ca_naics2_primary.py
import argparse
import pandas as pd
import numpy as np
from pathlib import Path
import re

def zfill_series(s, n):
    return s.astype(str).str.extract(r"(\d+)", expand=False).fillna("").str.zfill(n)

def normalize_abs_columns(df):
    lower = {c.lower(): c for c in df.columns}
    def pick(*opts):
        for o in opts:
            if o in lower:
                return lower[o]
        return None
    state  = pick("state","state_fips")
    county = pick("county","county_fips")
    naics  = pick("naics2022","naics")
    year   = pick("year")
    firms  = pick("firmpdemp","firms","firmpdem")
    emp    = pick("emp","employment")
    pay    = pick("payann","annual_payroll")
    rcpts  = pick("rcppdemp","receipts")
    need = [state, county, naics, firms, emp, pay, rcpts]
    if any(x is None for x in need):
        missing = [n for n,x in zip(["state","county","naics/naics2022","FIRMPDEMP","EMP","PAYANN","RCPPDEMP"], need) if x is None]
        raise ValueError(f"ABS file missing required columns (or synonyms): {missing}")
    df = df.rename(columns={
        state: "state",
        county: "county",
        naics: "naics_any",
        (firms or "FIRMPDEMP"): "FIRMPDEMP",
        (emp or "EMP"): "EMP",
        (pay or "PAYANN"): "PAYANN",
        (rcpts or "RCPPDEMP"): "RCPPDEMP",
    })
    if year: df = df.rename(columns={year: "year"})
    df["state"]  = zfill_series(df["state"], 2)
    df["county"] = zfill_series(df["county"], 3)
    df["naics2"] = df["naics_any"].astype(str).str.replace(r"\D","", regex=True).str[:2]
    for k in ["PAYANN","RCPPDEMP"]:
        df[k] = pd.to_numeric(df[k], errors="coerce") * 1000
    for c in ["FIRMPDEMP","EMP","PAYANN","RCPPDEMP"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")
    if "year" in df.columns:
        df["year"] = pd.to_numeric(df["year"], errors="coerce")
    df = df.rename(columns={
        "FIRMPDEMP":"abs_firms",
        "EMP":"abs_emp",
        "PAYANN":"abs_payroll_usd",
        "RCPPDEMP":"abs_receipts_usd"
    })
    return df

def build_abs_ca_naics2(abs_path, out_path, year=2022, state_fips="06"):
    abs_df = pd.read_csv(abs_path, dtype=str)
    abs_df = normalize_abs_columns(abs_df)
    abs_df = abs_df[abs_df["state"] == state_fips]
    if "year" in abs_df.columns and year is not None:
        abs_df = abs_df[abs_df["year"] == int(year)]
    abs_df = abs_df[abs_df["naics2"].str.fullmatch(r"\d{2}")].copy()
    key = ["state","county","naics2"]
    if "year" in abs_df.columns:
        key = key + ["year"]
    out = (abs_df.groupby(key, as_index=False)
              .agg(abs_firms=("abs_firms","sum"),
                   abs_emp=("abs_emp","sum"),
                   abs_payroll_usd=("abs_payroll_usd","sum"),
                   abs_receipts_usd=("abs_receipts_usd","sum")))
    assert out.duplicated(subset=key).sum() == 0
    for c in ["abs_firms","abs_emp","abs_payroll_usd","abs_receipts_usd"]:
        assert (out[c].dropna() >= 0).all()
    Path(out_path).parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(out_path, index=False)
    return out

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--abs", required=True, help="ABS county-level CSV (must include state/county and NAICS column)")
    ap.add_argument("--year", type=int, default=2022, help="Year to keep (if present)")
    ap.add_argument("--state_fips", default="06", help="State FIPS to filter (default CA=06)")
    ap.add_argument("--out", default="data/processed/ABS_2022_CA_allcounties_NAICS2.csv", help="Output CSV path")
    args = ap.parse_args()
    out = build_abs_ca_naics2(args.abs, args.out, year=args.year, state_fips=args.state_fips)
    print(f"Wrote {args.out} with {len(out):,} rows.")
if __name__ == "__main__":
    main()
