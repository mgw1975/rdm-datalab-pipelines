
# qcew_prep_naics_sector.py  (sector-level, robust ownership handling)
# -------------------------------------------------------------------
# Prepare BLS QCEW wages for county × NAICS **sector** × year.
# Filters:
#   - agg_lvl_cd == 74  (county-level NAICS sector rows)
#   - own_code: prefer '0' (Total). If not present, fallback to '1' (Private).
#   - state_cnty_fips_cd length == 5 (counties only)
#
# Output columns:
#   state_fips, county_fips, naics_sector, year,
#   annual_avg_emplvl, total_annual_wages, avg_weekly_wage
#
# Usage:
#   python scripts/qcew/qcew_prep_naics_sector.py \
#       --qcew_raw data_raw/qcew/2022.annual.singlefile.csv \
#       --year 2022 \
#       --out data_clean/qcew/econ_bnchmrk_qcew.csv
#
import argparse
from typing import Optional

import pandas as pd
import numpy as np

VALID_SECTORS = {
    "11","21","22","23","31-33","42","44-45","48-49","51","52","53","54",
    "55","56","61","62","71","72","81","92"
}

def derive_naics2(code: Optional[str]) -> Optional[str]:
    """
    Map raw NAICS codes to the canonical sector labels (11, 21, …, 31-33, 44-45, 48-49).
    Returns None for unsupported codes.
    """
    if not isinstance(code, str):
        return None
    code = code.strip()
    if not code or not code[0].isdigit():
        return None
    if len(code) == 2 and code.isdigit():
        base = code
    else:
        base = "".join(ch for ch in code if ch.isdigit())[:2]
        if len(base) < 2:
            return None
    if base in {"31", "32", "33"}:
        return "31-33"
    if base in {"44", "45"}:
        return "44-45"
    if base in {"48", "49"}:
        return "48-49"
    if base in {"11","21","22","23","42","51","52","53","54","55","56","61","62","71","72","81","92"}:
        return base
    return None

def normalize_qcew_columns(df):
    if df.columns.duplicated().any():
        df = df.loc[:, ~df.columns.duplicated()].copy()
    lower = {c.lower(): c for c in df.columns}
    def pick(*opts):
        for o in opts:
            if o in lower:
                return lower[o]
        return None
    area     = pick("area_fips","area","fips","state_cnty_fips_cd")
    ind      = pick("industry_code","naics","industry","indstr_cd")
    year_col = pick("year","year_num")
    aemp     = pick("annual_avg_emplvl","annual_avg_employment","annualaverageemployment","annual_avg_emplv","qcew_ann_avg_emp_lvl_num")
    twages   = pick("total_annual_wages","totalannualwages","annual_total_wages","tot_annual_wages","qcew_ttl_ann_wage_usd_amt")
    awage    = pick("avg_wkly_wage","avg_weekly_wage","average_weekly_wage","annual_avg_wkly_wage","qcew_avg_wkly_wage_usd_amt")
    agglvl   = pick("agglvl_code","agglevel_code","aggregation_level","agg_lvl_cd")
    own      = pick("own_code","ownership","own")
    qtr      = pick("qtr","quarter")
    need = [area, ind, year_col, aemp, twages, awage, agglvl]
    if any(x is None for x in need):
        missing = [n for n,x in zip(
            ["area_fips","industry_code","year","annual_avg_emplvl","total_annual_wages","avg_weekly_wage","agglvl_code"],
            need) if x is None]
        raise ValueError(f"Missing required columns (or synonyms): {missing}")
    df = df.rename(columns={
        area: "state_cnty_fips_cd",
        ind: "indstr_cd",
        year_col: "year_num",
        aemp: "qcew_ann_avg_emp_lvl_num",
        twages: "qcew_ttl_ann_wage_usd_amt",
        awage: "qcew_avg_wkly_wage_usd_amt",
        agglvl: "agg_lvl_cd"
    })
    if own:
        df = df.rename(columns={own: "own_code"})
    if qtr:
        df = df.rename(columns={qtr: "qtr"})
    return df

def prepare_qcew_sector(qdf, year=None, prefer_private_if_total_missing=True):
    df = qdf.copy()

    # Year
    if year is not None and "year_num" in df.columns:
        df = df[df["year_num"].astype(str) == str(year)]

    # Annual (if qtr present)
    if "qtr" in df.columns:
        df = df[df["qtr"].astype(str).str.upper().eq("A")]

   # Ownership: Use private (5) at NAICS-2. Total (0) is not reliably available.
    if "own_code" in df.columns:
        df["own_code"] = df["own_code"].astype(str).str.strip()
        df = df[df["own_code"] == "5"]
    else:
        # If no ownership column exists, assume private-only file (common in many extracts)
        df["own_code"] = "5"

    # Counties only
    df["state_cnty_fips_cd"] = df["state_cnty_fips_cd"].astype(str).str.zfill(5)
    df = df[df["state_cnty_fips_cd"].str.len() == 5].copy()
    if "agg_lvl_cd" in df.columns:
        df = df[df["agg_lvl_cd"].astype(str) == "74"].copy()

    # Sector labels
    df["indstr_cd"] = df["indstr_cd"].astype(str).str.strip()
    df["naics2_sector_cd"] = df["indstr_cd"].apply(derive_naics2)
    df = df[df["naics2_sector_cd"].notna()].copy()

    # Numerics
    for c in ["qcew_ann_avg_emp_lvl_num","qcew_ttl_ann_wage_usd_amt","qcew_avg_wkly_wage_usd_amt"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")
    if "year_num" in df.columns:
        df["year_num"] = pd.to_numeric(df["year_num"], errors="coerce")

    # Aggregate
    out = (df.groupby(["state_cnty_fips_cd","naics2_sector_cd","year_num","own_code"], as_index=False)
             .agg({
                 "qcew_ann_avg_emp_lvl_num":"sum",
                 "qcew_ttl_ann_wage_usd_amt":"sum"
             }))
    out["qcew_avg_wkly_wage_usd_amt"] = np.where(
        out["qcew_ann_avg_emp_lvl_num"] > 0,
        out["qcew_ttl_ann_wage_usd_amt"] / (out["qcew_ann_avg_emp_lvl_num"] * 52.0),
        np.nan
    )
    out["qcew_avg_wkly_wage_usd_amt"] = out["qcew_avg_wkly_wage_usd_amt"].replace([np.inf,-np.inf], np.nan).round(2)
    out["own_cd"] = out["own_code"]

    # QA
    assert out.duplicated(subset=["state_cnty_fips_cd","naics2_sector_cd","year_num","own_cd"]).sum() == 0
    for c in ["qcew_ann_avg_emp_lvl_num","qcew_ttl_ann_wage_usd_amt"]:
        assert (out[c].dropna() >= 0).all()

    return out[["year_num","naics2_sector_cd","state_cnty_fips_cd","own_cd",
                "qcew_ann_avg_emp_lvl_num","qcew_ttl_ann_wage_usd_amt","qcew_avg_wkly_wage_usd_amt"]]

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--qcew_raw", required=True, help="Path to raw QCEW annual CSV (singlefile preferred)")
    ap.add_argument("--year", type=int, default=2022, help="Year (default 2022)")
    ap.add_argument("--out", default="data_clean/qcew/econ_bnchmrk_qcew.csv", help="Output CSV path")
    args = ap.parse_args()

    raw = pd.read_csv(args.qcew_raw, dtype=str)
    raw = normalize_qcew_columns(raw)

    # Diagnostics
    diag = {
        "unique_years": sorted(raw["year_num"].astype(str).unique().tolist()) if "year_num" in raw.columns else None,
        "agglevel_counts": raw["agg_lvl_cd"].value_counts().to_dict() if "agg_lvl_cd" in raw.columns else None,
        "own_codes": sorted(raw["own_code"].astype(str).unique().tolist()) if "own_code" in raw.columns else None
    }
    print("Diagnostics:", diag)

    out = prepare_qcew_sector(raw, year=args.year, prefer_private_if_total_missing=True)
    out.to_csv(args.out, index=False)
    print(f"Wrote {args.out} with {len(out):,} rows.")

if __name__ == "__main__":
    main()
