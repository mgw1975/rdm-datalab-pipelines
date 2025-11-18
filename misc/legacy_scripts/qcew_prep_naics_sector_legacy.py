
# qcew_prep_naics_sector.py
# -------------------------------------------------------------
# Prepare BLS QCEW wages for county × NAICS **sector** × year.
# Uses QCEW aggregation level agglvl_code == 74 (County, NAICS Sector)
# and keeps official sector labels: 31-33, 44-45, 48-49, etc.
#
# OUTPUT columns:
#   state_fips, county_fips, naics_sector, year,
#   annual_avg_emplvl, total_annual_wages, avg_weekly_wage
#
# USAGE:
#   python qcew_prep_naics_sector.py \
#       --qcew_raw 2022_annual_singlefile.csv \
#       --year 2022 \
#       --out qcew_county_naics_sector_2022.csv
#
import argparse
import pandas as pd
import numpy as np

VALID_SECTORS = {
    "11","21","22","23","31-33","42","44-45","48-49","51","52","53","54",
    "55","56","61","62","71","72","81","92"
}

def normalize_qcew_columns(df):
    # Handle common column name variants
    if df.columns.duplicated().any():
        df = df.loc[:, ~df.columns.duplicated()].copy()
    lower = {c.lower(): c for c in df.columns}
    def pick(*opts):
        for o in opts:
            if o in lower:
                return lower[o]
        return None
    area     = pick("area_fips","area","fips")
    ind      = pick("industry_code","naics","industry")
    year_col = pick("year")
    aemp     = pick("annual_avg_emplvl","annual_avg_employment","annualaverageemployment","annual_avg_emplv")
    twages   = pick("total_annual_wages","totalannualwages","annual_total_wages","tot_annual_wages")
    awage    = pick("avg_wkly_wage","avg_weekly_wage","average_weekly_wage","annual_avg_wkly_wage")
    agglvl   = pick("agglvl_code","agglevel_code","aggregation_level")
    own      = pick("own_code","ownership","own")
    need = [area, ind, year_col, aemp, twages, awage, agglvl]
    if any(x is None for x in need):
        missing = [n for n,x in zip(
            ["area_fips","industry_code","year","annual_avg_emplvl","total_annual_wages","avg_weekly_wage","agglvl_code"],
            need) if x is None]
        raise ValueError(f"Missing required columns (or synonyms): {missing}")
    df = df.rename(columns={
        area: "area_fips",
        ind: "industry_code",
        year_col: "year",
        aemp: "annual_avg_emplvl",
        twages: "total_annual_wages",
        awage: "avg_weekly_wage",
        agglvl: "agglvl_code"
    })
    if own:
        df = df.rename(columns={own: "own_code"})
    return df

def prepare_qcew_sector(qdf, year=None, keep_own_code_zero=True):
    df = qdf.copy()

    # Filter year
    if year is not None and "year" in df.columns:
        df = df[df["year"].astype(str) == str(year)]

    # Keep county × NAICS sector rows only
    df = df[df["agglvl_code"].astype(str) == "74"]

    # Keep Total (all ownership) if present
    if keep_own_code_zero and "own_code" in df.columns:
        df = df[df["own_code"].astype(str) == "0"]

    # Counties only (area_fips length 5)
    df["area_fips"] = df["area_fips"].astype(str).str.strip()
    df = df[df["area_fips"].str.len() == 5].copy()
    df["state_fips"]  = df["area_fips"].str[:2]
    df["county_fips"] = df["area_fips"].str[2:]

    # Sector labels (keep official strings incl. ranges)
    df["industry_code"] = df["industry_code"].astype(str)
    df = df[df["industry_code"].isin(VALID_SECTORS)].copy()
    df["naics_sector"] = df["industry_code"]

    # Cast numerics
    for c in ["annual_avg_emplvl","total_annual_wages","avg_weekly_wage"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")
    if "year" in df.columns:
        df["year"] = pd.to_numeric(df["year"], errors="coerce")

    # Aggregate to unique county×sector×year
    grp = (df.groupby(["state_fips","county_fips","naics_sector","year"], as_index=False)
             .agg({
                 "annual_avg_emplvl": "sum",
                 "total_annual_wages": "sum"
             }))

    # Recompute avg weekly wage
    grp["avg_weekly_wage"] = np.where(
        grp["annual_avg_emplvl"] > 0,
        grp["total_annual_wages"] / (grp["annual_avg_emplvl"] * 52.0),
        np.nan
    )

    # QA
    assert grp.duplicated(subset=["state_fips","county_fips","naics_sector","year"]).sum() == 0
    for c in ["annual_avg_emplvl","total_annual_wages"]:
        assert (grp[c].dropna() >= 0).all()

    return grp[["state_fips","county_fips","naics_sector","year",
                "annual_avg_emplvl","total_annual_wages","avg_weekly_wage"]]

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--qcew_raw", required=True, help="Path to raw QCEW annual CSV (singlefile recommended)")
    ap.add_argument("--year", type=int, default=2022, help="Year (default 2022)")
    ap.add_argument("--out", default="qcew_county_naics_sector_2022.csv", help="Output CSV path")
    args = ap.parse_args()

    raw = pd.read_csv(args.qcew_raw, dtype=str)
    raw = normalize_qcew_columns(raw)
    out = prepare_qcew_sector(raw, year=args.year, keep_own_code_zero=True)
    out.to_csv(args.out, index=False)
    print(f"Wrote {args.out} with {len(out):,} rows.")

if __name__ == "__main__":
    main()
