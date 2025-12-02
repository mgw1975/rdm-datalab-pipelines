#!/usr/bin/env python3
"""
QA suite for merged ABS + QCEW dataset.

Loads portfolio_abs_qcew_ca_county_naics2.csv (county × NAICS2 × year)
and executes structural, numeric, and cross-source tests.
"""

from __future__ import annotations

import math
from pathlib import Path
from typing import List, Tuple

import numpy as np
import pandas as pd

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
DATA_PATH = Path("data_clean/integration/portfolio_abs_qcew_ca_county_naics2.csv")
SIMPLEMAPS_PATH = Path(
    "data_raw/external/simplemaps/simplemaps_uscounties_basicv1.91/uscounties.csv"
)
LOG_PATH = Path("outputs/qa/econ_bnchmrk_abs_qcew_qa.log")
FIPS_FAIL_PATH = Path("outputs/qa/econ_bnchmrk_abs_qcew_invalid_fips.csv")

NAICS2_VALID = [
    "11",
    "21",
    "22",
    "23",
    "31",
    "32",
    "33",
    "42",
    "44",
    "45",
    "48",
    "49",
    "51",
    "52",
    "53",
    "54",
    "55",
    "56",
    "61",
    "62",
    "71",
    "72",
    "81",
    "92",
]

# Ensure log directory exists
LOG_PATH.parent.mkdir(parents=True, exist_ok=True)


def log(msg: str) -> None:
    """Log helper that prints and writes to file."""
    print(msg)
    with LOG_PATH.open("a") as f:
        f.write(msg + "\n")


def load_dataset() -> pd.DataFrame:
    """Load the merged CSV (no header) and assign column names."""
    log("[LOAD] Reading merged ABS + QCEW CSV …")
    cols = [
        "cnty_nm",
        "geo_id",
        "naics2_sector_cd",
        "naics2_sector_desc",
        "ind_level_num",
        "abs_firm_num",
        "abs_emp_num",
        "abs_payroll_usd_amt",
        "abs_rcpt_usd_amt",
        "ind_level_num_dup",
        "state_fips_cd_raw",
        "cnty_fips_cd_raw",
        "naics2_sector_cd_dup",
        "qcew_ann_avg_emp_lvl_num",
        "qcew_ttl_ann_wage_usd_amt",
        "qcew_avg_wkly_wage_usd_amt",
        "year_num",
        "state_cnty_fips_cd_raw",
        "qcew_wage_per_emp_usd",
        "abs_wage_per_emp_usd",
        "abs_rcpt_per_firm_usd",
    ]
    df = pd.read_csv(DATA_PATH, header=None, names=cols, dtype=str)

    # Normalize numeric columns
    numeric_cols = [
        "abs_firm_num",
        "abs_emp_num",
        "abs_payroll_usd_amt",
        "abs_rcpt_usd_amt",
        "ind_level_num",
        "ind_level_num_dup",
        "qcew_ann_avg_emp_lvl_num",
        "qcew_ttl_ann_wage_usd_amt",
        "qcew_avg_wkly_wage_usd_amt",
        "year_num",
        "qcew_wage_per_emp_usd",
        "abs_wage_per_emp_usd",
        "abs_rcpt_per_firm_usd",
    ]
    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    # Build canonical state_cnty_fips_cd (zero-filled 5 digits)
    df["state_cnty_fips_cd"] = (
        df["state_cnty_fips_cd_raw"]
        .astype(str)
        .str.replace(r"\D", "", regex=True)
        .str.zfill(5)
    )

    # Use primary NAICS fields only
    df["naics2_sector_cd"] = df["naics2_sector_cd"].astype(str).str.zfill(2)
    df["year_num"] = df["year_num"].astype("Int64")

    # Derive missing ratios from base values
    df["abs_rcpt_per_emp_usd_amt"] = df["abs_rcpt_usd_amt"] / df["abs_emp_num"].replace(
        {0: np.nan}
    )
    df["qcew_wage_per_emp_usd"] = df["qcew_ttl_ann_wage_usd_amt"] / df[
        "qcew_ann_avg_emp_lvl_num"
    ].replace({0: np.nan})
    df["abs_wage_per_emp_usd"] = df["abs_payroll_usd_amt"] / df["abs_emp_num"].replace(
        {0: np.nan}
    )
    df["abs_rcpt_per_firm_usd"] = df["abs_rcpt_usd_amt"] / df["abs_firm_num"].replace(
        {0: np.nan}
    )

    # Reorder to canonical list
    keep = [
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
        "qcew_ann_avg_emp_lvl_num",
        "qcew_ttl_ann_wage_usd_amt",
        "qcew_avg_wkly_wage_usd_amt",
        "qcew_wage_per_emp_usd",
        "abs_wage_per_emp_usd",
        "abs_rcpt_per_firm_usd",
    ]
    df = df[keep]
    return df


def load_valid_fips() -> pd.DataFrame:
    """Load Simplemaps county list to validate state/county combinations."""
    ref = pd.read_csv(SIMPLEMAPS_PATH, dtype=str)
    ref["state_id"] = ref["state_id"].astype(str)
    ref["county_fips"] = ref["county_fips"].astype(str).str.zfill(5)
    return ref


def validate_fips(df: pd.DataFrame, ref: pd.DataFrame) -> Tuple[int, pd.DataFrame]:
    """Validate FIPS structure."""
    log("[QA] Structural Integrity: FIPS")
    failures = pd.DataFrame()
    mask_len = df["state_cnty_fips_cd"].str.fullmatch(r"\d{5}") == False
    if mask_len.any():
        log(f"  - Invalid length FIPS rows: {mask_len.sum()}")
        failures = pd.concat([failures, df[mask_len]])
    # Valid combos
    valid_set = set(ref["county_fips"])
    combo_mask = ~df["state_cnty_fips_cd"].isin(valid_set)
    if combo_mask.any():
        log(f"  - Unknown FIPS combos: {combo_mask.sum()}")
        bad = df[combo_mask].copy()
        failures = pd.concat([failures, bad])
        bad.to_csv(FIPS_FAIL_PATH, index=False)
        log(f"    → Details written to {FIPS_FAIL_PATH}")
    if failures.empty:
        log("  ✓ FIPS validation passed.")
    return len(failures), failures


def validate_naics(df: pd.DataFrame) -> int:
    log("[QA] Structural Integrity: NAICS2")
    mask_len = df["naics2_sector_cd"].str.fullmatch(r"\d{2}") == False
    mask_valid = ~df["naics2_sector_cd"].isin(NAICS2_VALID)
    total_fail = mask_len.sum() + mask_valid.sum()
    if mask_len.any():
        log(f"  - Non-2-digit NAICS rows: {mask_len.sum()}")
    if mask_valid.any():
        log(f"  - Invalid NAICS2 codes: {mask_valid.sum()}")
    if total_fail == 0:
        log("  ✓ NAICS validation passed.")
    return total_fail


def validate_years(df: pd.DataFrame) -> int:
    log("[QA] Structural Integrity: Year coverage")
    years = sorted(df["year_num"].dropna().unique())
    if not years:
        log("  - No year values found!")
        return 1
    log(f"  • Year range: {years[0]} – {years[-1]} ({len(years)} unique)")
    missing = 0
    if len(years) > 1:
        full = set(range(int(min(years)), int(max(years)) + 1))
        missing_years = sorted(full - set(int(y) for y in years))
        if missing_years:
            missing = len(missing_years)
            log(f"  - Missing years detected: {missing_years}")
    dupes = df.duplicated(subset=["year_num", "state_cnty_fips_cd", "naics2_sector_cd"])
    if dupes.any():
        log(f"  - Duplicate key rows: {dupes.sum()}")
        missing += dupes.sum()
    if missing == 0 and not dupes.any():
        log("  ✓ Year coverage OK.")
    return missing


def numeric_checks(df: pd.DataFrame) -> int:
    log("[QA] Numeric checks")
    fail_count = 0
    numeric_cols = [
        "abs_firm_num",
        "abs_emp_num",
        "abs_payroll_usd_amt",
        "abs_rcpt_usd_amt",
        "qcew_ann_avg_emp_lvl_num",
        "qcew_ttl_ann_wage_usd_amt",
        "qcew_avg_wkly_wage_usd_amt",
    ]
    for col in numeric_cols:
        invalid = df[col].dropna()
        negatives = invalid[invalid < 0]
        if negatives.any():
            log(f"  - Negative values in {col}: {len(negatives)}")
            fail_count += len(negatives)
    # Extreme value checks
    tmp = df.copy()
    tmp["abs_wage_per_emp_usd"] = (
        df["abs_payroll_usd_amt"] / df["abs_emp_num"].replace({0: np.nan})
    )
    tmp["qcew_wage_per_emp_usd"] = (
        df["qcew_ttl_ann_wage_usd_amt"]
        / df["qcew_ann_avg_emp_lvl_num"].replace({0: np.nan})
    )
    tmp["abs_rcpt_per_firm_usd"] = (
        df["abs_rcpt_usd_amt"] / df["abs_firm_num"].replace({0: np.nan})
    )
    ranges = [
        ("abs_wage_per_emp_usd", 10_000, 500_000),
        ("qcew_wage_per_emp_usd", 10_000, 500_000),
        ("abs_rcpt_per_firm_usd", 0, 50_000_000),
    ]
    for col, lo, hi in ranges:
        bad = tmp[(tmp[col].notna()) & ((tmp[col] < lo) | (tmp[col] > hi))]
        if not bad.empty:
            log(f"  - {col} out-of-range rows: {len(bad)}")
            fail_count += len(bad)
    return fail_count


def cross_source_checks(df: pd.DataFrame) -> int:
    log("[QA] Cross-source consistency")
    fail = 0
    abs_wage = df["abs_payroll_usd_amt"] / df["abs_emp_num"].replace({0: np.nan})
    qcew_wage = df["qcew_ttl_ann_wage_usd_amt"] / df["qcew_ann_avg_emp_lvl_num"].replace(
        {0: np.nan}
    )
    df["abs_wage_calc"] = abs_wage
    df["qcew_wage_calc"] = qcew_wage
    mask = abs_wage.notna() & qcew_wage.notna()
    if mask.any():
        corr = abs_wage[mask].corr(qcew_wage[mask])
        log(f"  • Wage-per-employee correlation: {corr:.3f}")
    ratio = abs_wage / qcew_wage
    ratio_flags = df[(mask) & ((ratio < 0.2) | (ratio > 5))]
    if not ratio_flags.empty:
        log(f"  - ABS/QCEW wage ratio outliers: {len(ratio_flags)}")
        fail += len(ratio_flags)
    expected_weekly = qcew_wage / 52
    diff = (
        (df["qcew_avg_wkly_wage_usd_amt"] - expected_weekly).abs()
        / expected_weekly.replace({0: np.nan}).abs()
    )
    weekly_flags = df[(expected_weekly.notna()) & (diff > 0.10)]
    if not weekly_flags.empty:
        log(f"  - QCEW weekly wage mismatch rows: {len(weekly_flags)}")
        fail += len(weekly_flags)
    return fail


def coverage_checks(df: pd.DataFrame) -> None:
    log("[QA] Coverage comparisons")
    abs_missing_qcew = df[
        df["abs_firm_num"].notna() & df["qcew_ann_avg_emp_lvl_num"].isna()
    ]
    qcew_missing_abs = df[
        df["abs_firm_num"].isna() & df["qcew_ann_avg_emp_lvl_num"].notna()
    ]
    log(f"  • ABS rows lacking QCEW data: {len(abs_missing_qcew)}")
    log(f"  • QCEW rows lacking ABS data: {len(qcew_missing_abs)}")


def quantiles_and_outliers(df: pd.DataFrame) -> None:
    log("[QA] Distribution summaries")
    metrics = {
        "abs_wage_per_emp_usd": df["abs_payroll_usd_amt"]
        / df["abs_emp_num"].replace({0: np.nan}),
        "qcew_wage_per_emp_usd": df["qcew_ttl_ann_wage_usd_amt"]
        / df["qcew_ann_avg_emp_lvl_num"].replace({0: np.nan}),
        "abs_rcpt_per_firm_usd": df["abs_rcpt_usd_amt"]
        / df["abs_firm_num"].replace({0: np.nan}),
    }
    for name, series in metrics.items():
        series = series.dropna()
        if series.empty:
            continue
        q = series.quantile([0.01, 0.05, 0.95, 0.99])
        log(f"  • {name} quantiles: {q.to_dict()}")
        # Z-score outliers
        z = (series - series.mean()) / series.std(ddof=0)
        outliers = z.abs().sort_values(ascending=False).head(20)
        log(f"  • Top {len(outliers)} z-score outliers for {name}:")
        log(outliers.to_string())


def main() -> None:
    if LOG_PATH.exists():
        LOG_PATH.unlink()
    df = load_dataset()
    ref = load_valid_fips()

    total_failures = 0
    failures = 0
    failures += validate_fips(df, ref)[0]
    failures += validate_naics(df)
    failures += validate_years(df)
    # Row uniqueness already partly checked in validate_years
    duplicates = df.duplicated(subset=["year_num", "state_cnty_fips_cd", "naics2_sector_cd"])
    if duplicates.any():
        log(f"[QA] Row uniqueness violation count: {duplicates.sum()}")
        failures += duplicates.sum()
    else:
        log("[QA] Row uniqueness check passed.")
    failures += numeric_checks(df)
    failures += cross_source_checks(df)
    coverage_checks(df)
    quantiles_and_outliers(df)

    total_failures = failures
    status = "PASS" if total_failures == 0 else "FAIL"
    log(f"[QA SUMMARY] Overall: {status} (issues found: {total_failures})")


if __name__ == "__main__":
    main()
