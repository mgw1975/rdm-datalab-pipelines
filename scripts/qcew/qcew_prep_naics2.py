
# qcew_prep_naics2.py
# -------------------------------------------------------------
# Prepare BLS QCEW wages for portfolio use at:
#   county × 2-digit NAICS × year
#
# INPUT: Raw QCEW CSV (annual preferred) that includes:
#   - area_fips   (5-digit county FIPS; states are 2-digit, counties 3-digit)
#   - industry_code (NAICS; we'll derive naics2 from the first 2 digits)
#   - year
#   - annual_avg_emplvl
#   - total_annual_wages
#   - avg_wkly_wage  (or avg_weekly_wage; both handled)
#   - Optional: own_code (we'll keep own_code == '0' if present — 'Total covered')
#
# OUTPUT: qcew_county_naics2_YYYY.csv
#   Columns: state_fips, county_fips, naics2, year,
#            annual_avg_emplvl, total_annual_wages, avg_weekly_wage
#
# USAGE:
#   python qcew_prep_naics2.py \
#       --qcew_raw /path/to/qcew_annual_raw_2022.csv \
#       --year 2022 \
#       --out qcew_county_naics2_2022.csv
#
import argparse
import pandas as pd
import numpy as np
import re

def zfill_series(s, n):
    return s.astype(str).str.extract(r"(\d+)", expand=False).fillna("").str.zfill(n)

def normalize_qcew_columns(df):
    # Standardize key column names with flexible matching
    lower = {c.lower(): c for c in df.columns}
    def pick(*opts):
        for o in opts:
            if o in lower:
                return lower[o]
        return None

    area     = pick("area_fips", "area", "fips")
    ind      = pick("industry_code", "naics", "industry")
    year_col = pick("year")
    aemp     = pick("annual_avg_emplvl", "annual_avg_employment", "annualaverageemployment", "annual_avg_emplv")
    twages   = pick("total_annual_wages", "totalannualwages", "annual_total_wages", "tot_annual_wages")
    awage    = pick("avg_wkly_wage", "avg_weekly_wage", "average_weekly_wage")
    own      = pick("own_code", "ownership", "own")

    need = [area, ind, year_col, aemp, twages, awage]
    if any(x is None for x in need):
        missing = [n for n,x in zip(
            ["area_fips","industry_code","year","annual_avg_emplvl","total_annual_wages","avg_weekly_wage"],
            need
        ) if x is None]
        raise ValueError(f"QCEW file missing required columns (or synonyms): {missing}")

    df = df.rename(columns={
        area: "area_fips",
        ind: "industry_code",
        year_col: "year",
        aemp: "annual_avg_emplvl",
        twages: "total_annual_wages",
        awage: "avg_weekly_wage",
    })
    if own:
        df = df.rename(columns={own: "own_code"})
    return df

def prepare_qcew_naics2(qdf, year=None, keep_own_code_zero=True):
    df = qdf.copy()
    # Filter year
    if year is not None and "year" in df.columns:
        df = df[df["year"].astype(str) == str(year)]
    # Optionally filter to ownership code '0' (total covered, if present)
    if keep_own_code_zero and "own_code" in df.columns:
        df = df[df["own_code"].astype(str) == "0"]
    # Keep only county-level area_fips (5 digits)
    df["area_fips"] = df["area_fips"].astype(str).str.strip()
    df = df[df["area_fips"].str.len() == 5]
    df["state_fips"]  = df["area_fips"].str[:2]
    df["county_fips"] = df["area_fips"].str[2:]
    # Derive NAICS2 (digits only) and keep only 2-digit codes
    df["naics2"] = df["industry_code"].astype(str).str.extract(r"(\d+)", expand=False)
    df["naics2"] = df["naics2"].str[:2]
    df = df[df["naics2"].str.len() == 2]
    # Cast numerics
    for c in ["annual_avg_emplvl","total_annual_wages","avg_weekly_wage"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")
    if "year" in df.columns:
        df["year"] = pd.to_numeric(df["year"], errors="coerce")

    # Some files include multiple rows per county×naics2×year (e.g., due to sizing or classes).
    # Collapse to one record per key with sensible aggregations:
    #  - employment: sum over classes (approximation)
    #  - total wages: sum
    #  - avg weekly wage: recompute as total_wages / (employment * 52) when possible
    grp = (df.groupby(["state_fips","county_fips","naics2","year"], as_index=False)
             .agg({
                 "annual_avg_emplvl": "sum",
                 "total_annual_wages": "sum"
             }))
    grp["avg_weekly_wage"] = np.where(
        grp["annual_avg_emplvl"] > 0,
        grp["total_annual_wages"] / (grp["annual_avg_emplvl"] * 52.0),
        np.nan
    )
    # Order columns
    out = grp[["state_fips","county_fips","naics2","year","annual_avg_emplvl","total_annual_wages","avg_weekly_wage"]]
    return out

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--qcew_raw", required=True, help="Path to raw QCEW CSV (annual preferred)")
    ap.add_argument("--year", type=int, default=2022, help="Year to keep (default 2022)")
    ap.add_argument("--out", default="qcew_county_naics2_2022.csv", help="Output CSV path")
    args = ap.parse_args()

    raw = pd.read_csv(args.qcew_raw, dtype=str)
    raw = normalize_qcew_columns(raw)
    out = prepare_qcew_naics2(raw, year=args.year, keep_own_code_zero=True)
    out.to_csv(args.out, index=False)
    print(f"Wrote {args.out} with {len(out):,} rows.")

if __name__ == "__main__":
    main()
