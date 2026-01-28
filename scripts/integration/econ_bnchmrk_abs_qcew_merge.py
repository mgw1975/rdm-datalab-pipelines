#!/usr/bin/env python3
"""
Merge multi-year ABS + QCEW extracts and attach population metadata.

Reads per-year ABS and QCEW CSVs (produced by scripts/abs/econ_bnchmrk_abs.py and
scripts/qcew/qcew_prep_naics2.py), joins them on year/FIPS/NAICS2, derives cross-
source metrics, enriches with population from ref_state_cnty_uscb, and writes a
stacked county × NAICS2 × year file for downstream analytics. For the MVP we
ship a post-pandemic panel (2022–2023); older ABS vintages are intentionally
out-of-scope.
"""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import List

import numpy as np
import pandas as pd

ABS_PATTERN_DEFAULT = "data_clean/abs/econ_bnchmrk_abs_{year}.csv"
QCEW_PATTERN_DEFAULT = "data_clean/qcew/econ_bnchmrk_qcew_{year}.csv"
REF_DEFAULT = "data_clean/reference/ref_state_cnty_uscb.csv"
OUT_DEFAULT = "data_clean/integration/econ_bnchmrk_abs_qcew.csv"
MVP_YEARS = [2022, 2023]


def safe_divide(num: pd.Series, den: pd.Series) -> pd.Series:
    denom = den.replace({0: np.nan})
    return num / denom


def load_abs(year: int, pattern: str) -> pd.DataFrame:
    path = Path(pattern.format(year=year))
    if not path.exists():
        raise FileNotFoundError(f"ABS file missing for {year}: {path}")
    df = pd.read_csv(path, dtype={"state_cnty_fips_cd": str, "naics2_sector_cd": str})
    df["year_num"] = year
    return df


def load_qcew(year: int, pattern: str) -> pd.DataFrame:
    path = Path(pattern.format(year=year))
    if not path.exists():
        raise FileNotFoundError(f"QCEW file missing for {year}: {path}")
    df = pd.read_csv(path, dtype={"state_cnty_fips_cd": str, "naics2_sector_cd": str})
    df["year_num"] = year
    return df


def merge_year(abs_df: pd.DataFrame, qcew_df: pd.DataFrame) -> pd.DataFrame:
    """Merge ABS + QCEW for a single year."""
    merge_cols = ["year_num", "state_cnty_fips_cd", "naics2_sector_cd"]
    merged = abs_df.merge(qcew_df, how="outer", on=merge_cols, suffixes=("", "_qcew"))
    merged["state_fips_cd"] = merged["state_cnty_fips_cd"].str[:2]
    if "state_fips_cd_qcew" in merged.columns:
        merged["state_fips_cd"] = merged["state_fips_cd"].fillna(
            merged["state_fips_cd_qcew"]
        )
    return merged


def enrich_population(df: pd.DataFrame, ref_path: Path) -> pd.DataFrame:
    ref = pd.read_csv(
        ref_path,
        dtype={"state_cnty_fips_cd": str},
        usecols=lambda c: c
        in {"state_cnty_fips_cd", "state_cd", "cnty_nm", "population_num", "population_year"},
    )
    merged = df.merge(ref, on="state_cnty_fips_cd", how="left", suffixes=("", "_ref"))
    merged["cnty_nm"] = merged["cnty_nm"].fillna(merged.get("cnty_nm_ref"))
    merged = merged.drop(columns=[c for c in merged.columns if c.endswith("_ref")])
    return merged


def derive_metrics(df: pd.DataFrame) -> pd.DataFrame:
    """Compute per-employee / per-firm ratios with graceful NaN handling."""
    for col in [
        "abs_firm_num",
        "abs_emp_num",
        "abs_payroll_usd_amt",
        "abs_rcpt_usd_amt",
        "qcew_ann_avg_emp_lvl_num",
        "qcew_ttl_ann_wage_usd_amt",
    ]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    df["abs_rcpt_per_emp_usd_amt"] = safe_divide(df["abs_rcpt_usd_amt"], df["abs_emp_num"])
    df["abs_wage_per_emp_usd_amt"] = safe_divide(df["abs_payroll_usd_amt"], df["abs_emp_num"])
    df["abs_rcpt_per_firm_usd_amt"] = safe_divide(df["abs_rcpt_usd_amt"], df["abs_firm_num"])
    df["qcew_wage_per_emp_usd_amt"] = safe_divide(
        df["qcew_ttl_ann_wage_usd_amt"], df["qcew_ann_avg_emp_lvl_num"]
    )
    return df


def assemble(
    years: List[int],
    abs_pattern: str,
    qcew_pattern: str,
    ref_csv: Path,
) -> pd.DataFrame:
    frames = []
    for year in years:
        abs_df = load_abs(year, abs_pattern)
        qcew_df = load_qcew(year, qcew_pattern)
        merged = merge_year(abs_df, qcew_df)
        frames.append(merged)
    combined = pd.concat(frames, ignore_index=True)
    combined = derive_metrics(combined)
    combined = enrich_population(combined, ref_csv)
    combined = combined.sort_values(
        ["year_num", "state_cnty_fips_cd", "naics2_sector_cd"]
    ).reset_index(drop=True)

    dupes = combined.duplicated(
        subset=["year_num", "state_cnty_fips_cd", "naics2_sector_cd"]
    ).sum()
    if dupes:
        raise AssertionError(f"Duplicate merged rows detected: {dupes}")
    return combined


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Merge multi-year ABS + QCEW extracts with population."
    )
    parser.add_argument(
        "--years",
        type=int,
        nargs="+",
        help="Years to include (default: MVP years).",
    )
    parser.add_argument(
        "--abs_pattern",
        default=ABS_PATTERN_DEFAULT,
        help="ABS per-year path pattern (default: %(default)s)",
    )
    parser.add_argument(
        "--qcew_pattern",
        default=QCEW_PATTERN_DEFAULT,
        help="QCEW per-year path pattern (default: %(default)s)",
    )
    parser.add_argument(
        "--ref_csv",
        default=REF_DEFAULT,
        help="Reference CSV with population (default: %(default)s)",
    )
    parser.add_argument(
        "--out",
        default=OUT_DEFAULT,
        help="Destination for merged dataset (default: %(default)s)",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    years = sorted(set(args.years)) if args.years else MVP_YEARS.copy()
    ref_path = Path(args.ref_csv)
    merged = assemble(years, args.abs_pattern, args.qcew_pattern, ref_path)
    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    merged.to_csv(out_path, index=False)
    print(f"[MERGE] Wrote merged dataset: {out_path} ({len(merged):,} rows).")


if __name__ == "__main__":
    main()
