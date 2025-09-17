
# rdm_abs_naics3_cbsa.py
# -------------------------------------------------------------
# ABS (county × NAICS3) → reconcile against NAICS 00 totals per county,
# then aggregate to CBSA and optionally filter to "large" CBSAs.
#
# OUTPUTS (written to --outdir):
#   - abs_county_naics3_recon_report.csv
#   - abs_cbsa_naics3.csv
#   - abs_cbsa_naics3_large.csv
#   - abs_cbsa_naics3_discrepancies.csv
#
import argparse
import pandas as pd
import numpy as np
from pathlib import Path
import re

def zfill_series(s, n):
    return s.astype(str).str.extract(r"(\d+)", expand=False).fillna("").str.zfill(n)

def normalize_abs_columns(df):
    # Flexible rename for common ABS headers
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
        missing = [n for n,x in zip(
            ["state","county","naics/naics2022","FIRMPDEMP","EMP","PAYANN","RCPPDEMP"], need
        ) if x is None]
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

    # NAICS3
    df["naics3"] = df["naics_any"].astype(str).str.replace(r"\D","", regex=True).str[:3]
    # Mark totals: NAICS 00 or 000
    is_total = df["naics_any"].astype(str).str.fullmatch(r"0+|00") | df["naics3"].isin(["000","00"])
    df.loc[is_total, "naics3"] = "000"

    # FIPS tidy
    df["state"]  = zfill_series(df["state"], 2)
    df["county"] = zfill_series(df["county"], 3)

    # Convert $1,000s → dollars for PAYANN/RCPPDEMP
    for k in ["PAYANN","RCPPDEMP"]:
        df[k] = pd.to_numeric(df[k], errors="coerce") * 1000

    # Numerics
    for c in ["FIRMPDEMP","EMP","PAYANN","RCPPDEMP"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")
    if "year" in df.columns:
        df["year"] = pd.to_numeric(df["year"], errors="coerce")

    return df

def normalize_crosswalk(df):
    need = ["state_fips","county_fips","cbsa_code","cbsa_title"]
    missing = [c for c in need if c not in df.columns]
    if missing:
        raise ValueError(f"Crosswalk missing columns: {missing}")
    df["state_fips"]  = zfill_series(df["state_fips"], 2)
    df["county_fips"] = zfill_series(df["county_fips"], 3)
    if "cbsa_pop" in df.columns:
        df["cbsa_pop"] = pd.to_numeric(df["cbsa_pop"], errors="coerce")
    return df

def reconcile_county_totals(abs_df, year=None, atol=1.0):
    base = abs_df.copy()
    if year is not None and "year" in base.columns:
        base = base[base["year"] == year]

    # Split totals vs parts
    all_rows = base[base["naics3"] == "000"].copy()
    parts    = base[base["naics3"] != "000"].copy()

    key = ["state","county"]
    sums = (parts.groupby(key, as_index=False)[["FIRMPDEMP","EMP","PAYANN","RCPPDEMP"]]
                .sum(min_count=1))
    totals = (all_rows.groupby(key, as_index=False)[["FIRMPDEMP","EMP","PAYANN","RCPPDEMP"]]
                    .sum(min_count=1))

    # Rename for clarity
    sums = sums.rename(columns={"FIRMPDEMP":"sum_firms","EMP":"sum_emp","PAYANN":"sum_pay","RCPPDEMP":"sum_rcpts"})
    totals = totals.rename(columns={"FIRMPDEMP":"tot_firms","EMP":"tot_emp","PAYANN":"tot_pay","RCPPDEMP":"tot_rcpts"})

    rep = sums.merge(totals, on=key, how="outer")

    for c in [("firms","sum_firms","tot_firms"),
              ("emp","sum_emp","tot_emp"),
              ("pay","sum_pay","tot_pay"),
              ("rcpts","sum_rcpts","tot_rcpts")]:
        name, s_col, t_col = c
        rep[f"delta_{name}"] = rep[s_col] - rep[t_col]
        rep[f"pct_delta_{name}"] = np.where(rep[t_col].abs() > 0, rep[f"delta_{name}"]/rep[t_col], np.nan)
        rep[f"flag_{name}"] = rep[f"delta_{name}"].abs() > atol

    rep["recon_ok"] = ~(rep[[f"flag_{c}" for c in ["firms","emp","pay","rcpts"]]].any(axis=1))
    return rep.sort_values(key)

def aggregate_to_cbsa(abs_df, xwalk_df, year=None):
    base = abs_df.copy()
    if year is not None and "year" in base.columns:
        base = base[base["year"] == year]
    base["state_fips"]  = base["state"]
    base["county_fips"] = base["county"]
    merged = base.merge(xwalk_df, on=["state_fips","county_fips"], how="left", validate="m:1")

    # Non-total NAICS3 → CBSA × NAICS3
    parts = merged[merged["naics3"] != "000"].copy()
    key = ["cbsa_code","cbsa_title","naics3"]
    out = (parts.groupby(key, as_index=False)[["FIRMPDEMP","EMP","PAYANN","RCPPDEMP"]]
                .sum(min_count=1))

    # CBSA totals from county totals
    all_cbsa = (merged[merged["naics3"] == "000"]
                .groupby(["cbsa_code","cbsa_title"], as_index=False)[["FIRMPDEMP","EMP","PAYANN","RCPPDEMP"]]
                .sum(min_count=1)
                .rename(columns={
                    "FIRMPDEMP":"cbsa_tot_firms",
                    "EMP":"cbsa_tot_emp",
                    "PAYANN":"cbsa_tot_payroll",
                    "RCPPDEMP":"cbsa_tot_receipts"
                }))
    out = out.merge(all_cbsa, on=["cbsa_code","cbsa_title"], how="left")
    return out

def filter_large_cbsa(cbsa_df, xwalk_df, large_by="firms", threshold=20000):
    if large_by == "population" and "cbsa_pop" in xwalk_df.columns:
        pop = xwalk_df.drop_duplicates(subset=["cbsa_code","cbsa_title"])[["cbsa_code","cbsa_title","cbsa_pop"]]
        cbsa_totals = (cbsa_df.drop_duplicates(subset=["cbsa_code","cbsa_title"])[["cbsa_code","cbsa_title"]]
                            .merge(pop, on=["cbsa_code","cbsa_title"], how="left"))
        big_codes = cbsa_totals.loc[cbsa_totals["cbsa_pop"] >= threshold, ["cbsa_code","cbsa_title"]]
    else:
        totals = (cbsa_df.groupby(["cbsa_code","cbsa_title"], as_index=False)["cbsa_tot_firms"].max())
        big_codes = totals.loc[totals["cbsa_tot_firms"] >= threshold, ["cbsa_code","cbsa_title"]]
    return cbsa_df.merge(big_codes, on=["cbsa_code","cbsa_title"], how="inner")

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--abs", required=True, help="ABS county×NAICS3 CSV")
    ap.add_argument("--xwalk", required=True, help="County→CBSA crosswalk CSV")
    ap.add_argument("--year", type=int, default=None, help="Year filter (optional)")
    ap.add_argument("--large_by", choices=["firms","population"], default="firms")
    ap.add_argument("--large_threshold", type=int, default=20000)
    ap.add_argument("--outdir", default="data/processed")
    ap.add_argument("--recon_atol", type=float, default=1.0, help="Absolute tolerance for recon deltas")
    args = ap.parse_args()

    outdir = Path(args.outdir); outdir.mkdir(parents=True, exist_ok=True)

    abs_df = pd.read_csv(args.abs, dtype=str)
    abs_df = normalize_abs_columns(abs_df)
    xw = pd.read_csv(args.xwalk, dtype=str)
    xw = normalize_crosswalk(xw)

    # Reconcile
    recon = reconcile_county_totals(abs_df, year=args.year, atol=args.recon_atol)
    recon.to_csv(outdir / "abs_county_naics3_recon_report.csv", index=False)

    # Aggregate
    cbsa = aggregate_to_cbsa(abs_df, xw, year=args.year)
    cbsa.to_csv(outdir / "abs_cbsa_naics3.csv", index=False)

    # Large CBSA filter
    cbsa_large = filter_large_cbsa(cbsa, xw, large_by=args.large_by, threshold=args.large_threshold)
    cbsa_large.to_csv(outdir / "abs_cbsa_naics3_large.csv", index=False)

    # Discrepancies joined to CBSA context
    cw_small = xw.rename(columns={"state_fips":"state","county_fips":"county"})
    bad = recon[~recon["recon_ok"]].merge(cw_small, on=["state","county"], how="left")
    bad.to_csv(outdir / "abs_cbsa_naics3_discrepancies.csv", index=False)

    print("Wrote outputs to", outdir.resolve())

if __name__ == "__main__":
    main()
