#!/usr/bin/env python3
"""
econ_bnchmrk_qcew.py
--------------------
Prepare county × NAICS2 × year QCEW benchmarks with private ownership only.

Features:
  - Handles per-year or multi-year batches with templated input/output paths.
  - Normalizes flexible QCEW column names and enforces county-level rows.
  - Filters to own_code == "5" (private) and NAICS sector grain (agg level 74).
  - Writes outputs that match the econ_bnchmrk_qcew BigQuery schema, including
    state/county splits and NUMERIC precision-friendly wage calculations.

Usage:
  # Default MVP years (2022–2023)
  python scripts/qcew/econ_bnchmrk_qcew.py

  # Explicit years and single raw file (e.g., unit tests)
  python scripts/qcew/econ_bnchmrk_qcew.py \
      --years 2022 2023 \
      --raw_template data_raw/qcew/{year}.annual.singlefile.csv \
      --per_year_pattern data_clean/qcew/econ_bnchmrk_qcew_{year}.csv \
      --out data_clean/qcew/econ_bnchmrk_qcew_multiyear.csv
"""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd

NUMERIC_PRECISION = 9
MVP_YEARS = [2022, 2023]
DEFAULT_RAW_TEMPLATE = "data_raw/qcew/{year}.annual.singlefile.csv"
DEFAULT_PER_YEAR_PATTERN = "data_clean/qcew/econ_bnchmrk_qcew_{year}.csv"
DEFAULT_STACKED_OUT = "data_clean/qcew/econ_bnchmrk_qcew_multiyear.csv"

# Canonical NAICS2 buckets we support downstream. This doubles as an explicit
# allowlist so any surprise NAICS codes in the raw input will be dropped during
# prep instead of leaking into BigQuery.
VALID_SECTORS = {
    "11",
    "21",
    "22",
    "23",
    "31-33",
    "42",
    "44-45",
    "48-49",
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
}


def derive_naics2(code: Optional[str]) -> Optional[str]:
    """Map raw NAICS codes to canonical sector labels."""
    if not isinstance(code, str):
        return None
    cleaned = "".join(ch for ch in code.strip() if ch.isdigit())
    if len(cleaned) < 2:
        return None
    base = cleaned[:2]
    if base in {"31", "32", "33"}:
        return "31-33"
    if base in {"44", "45"}:
        return "44-45"
    if base in {"48", "49"}:
        return "48-49"
    if base in {
        "11",
        "21",
        "22",
        "23",
        "42",
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
    }:
        return base
    return None


def normalize_qcew_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Standardize key column names with flexible matching."""
    frame = df.copy()
    if frame.columns.duplicated().any():
        frame = frame.loc[:, ~frame.columns.duplicated()].copy()

    lower = {c.lower(): c for c in frame.columns}

    def pick(*opts: str) -> Optional[str]:
        for opt in opts:
            if opt in lower:
                return lower[opt]
        return None

    # Normalize core identifiers and metrics. Most QCEW annual single files use
    # the canonical names, but the helper makes the script resilient to future
    # renames or alternate exports (e.g., parity with legacy ABS mashups).
    area = pick("state_cnty_fips_cd", "area_fips", "area", "fips")
    ind = pick("indstr_cd", "industry_code", "naics", "industry")
    year_col = pick("year_num", "year")
    aemp = pick(
        "qcew_ann_avg_emp_lvl_num",
        "annual_avg_emplvl",
        "annual_avg_employment",
        "annualaverageemployment",
        "annual_avg_emplv",
    )
    twages = pick(
        "qcew_ttl_ann_wage_usd_amt",
        "total_annual_wages",
        "totalannualwages",
        "annual_total_wages",
        "tot_annual_wages",
    )
    awage = pick(
        "qcew_avg_wkly_wage_usd_amt",
        "avg_wkly_wage",
        "avg_weekly_wage",
        "average_weekly_wage",
        "annual_avg_wkly_wage",
    )
    agglvl = pick("agg_lvl_cd", "agglvl_code", "aggregation_level")
    own = pick("own_code", "ownership", "own")
    qtr = pick("qtr", "quarter")

    need = [area, ind, year_col, aemp, twages, awage, agglvl]
    if any(val is None for val in need):
        missing = [
            label
            for label, val in zip(
                [
                    "area_fips",
                    "industry_code",
                    "year",
                    "annual_avg_emplvl",
                    "total_annual_wages",
                    "avg_weekly_wage",
                    "agg_lvl_cd",
                ],
                need,
            )
            if val is None
        ]
        raise ValueError(f"QCEW file missing required columns (or synonyms): {missing}")

    rename_map = {
        area: "state_cnty_fips_cd",
        ind: "indstr_cd",
        year_col: "year_num",
        aemp: "qcew_ann_avg_emp_lvl_num",
        twages: "qcew_ttl_ann_wage_usd_amt",
        awage: "qcew_avg_wkly_wage_usd_amt",
        agglvl: "agg_lvl_cd",
    }
    frame = frame.rename(columns=rename_map)
    if own:
        frame = frame.rename(columns={own: "own_code"})
    if qtr:
        frame = frame.rename(columns={qtr: "qtr"})
    return frame


def prepare_qcew_private(df: pd.DataFrame, year: int) -> pd.DataFrame:
    """Filter normalized QCEW data to private NAICS2 sectors for one year."""
    working = df.copy()
    # Restrict the normalized frame to the active year. We keep year as string
    # comparisons here to avoid dropping rows that were parsed as floats in
    # funky source files, and convert to numeric later.
    working = working[working["year_num"].astype(str) == str(year)]

    # Annual-only: drop quarterly rows when the raw file includes them. BLS
    # annual single files already default to "A" but some custom dumps do not.
    if "qtr" in working.columns:
        working = working[working["qtr"].astype(str).str.upper() == "A"]

    # Ownership: enforce private (own_code == "5"). If the column is missing we
    # assume the source is already private-only and backfill the metadata so
    # downstream merges still know which cohort they are seeing.
    if "own_code" in working.columns:
        working["own_code"] = working["own_code"].astype(str).str.strip()
        working = working[working["own_code"] == "5"]
    else:
        working["own_code"] = "5"

    working["state_cnty_fips_cd"] = working["state_cnty_fips_cd"].astype(str).str.zfill(5)
    working = working[working["state_cnty_fips_cd"].str.len() == 5].copy()
    working["state_fips_cd"] = working["state_cnty_fips_cd"].str[:2]
    working["cnty_fips_cd"] = working["state_cnty_fips_cd"].str[2:]

    # Only keep the county × NAICS sector rows (agg level 74). Anything else
    # would either double-count (state totals) or add the detail records the
    # aggregation is about to roll up anyway.
    if "agg_lvl_cd" in working.columns:
        working = working[working["agg_lvl_cd"].astype(str) == "74"].copy()

    working["indstr_cd"] = working["indstr_cd"].astype(str).str.strip()
    working["naics2_sector_cd"] = working["indstr_cd"].apply(derive_naics2)
    working = working[working["naics2_sector_cd"].isin(VALID_SECTORS)].copy()

    for col in [
        "qcew_ann_avg_emp_lvl_num",
        "qcew_ttl_ann_wage_usd_amt",
        "qcew_avg_wkly_wage_usd_amt",
        "year_num",
    ]:
        working[col] = pd.to_numeric(working[col], errors="coerce")

    group_cols = [
        "year_num",
        "state_cnty_fips_cd",
        "state_fips_cd",
        "cnty_fips_cd",
        "naics2_sector_cd",
        "own_code",
    ]
    grouped = (
        working.groupby(group_cols, as_index=False)
        .agg(
            {
                "qcew_ann_avg_emp_lvl_num": "sum",
                "qcew_ttl_ann_wage_usd_amt": "sum",
            }
        )
    )
    # Recompute average weekly wage after summing employment/wage totals so the
    # ratios stay internally consistent with the aggregate employment counts.
    grouped["qcew_avg_wkly_wage_usd_amt"] = np.where(
        grouped["qcew_ann_avg_emp_lvl_num"] > 0,
        grouped["qcew_ttl_ann_wage_usd_amt"] / (grouped["qcew_ann_avg_emp_lvl_num"] * 52.0),
        np.nan,
    )
    grouped["qcew_avg_wkly_wage_usd_amt"] = grouped["qcew_avg_wkly_wage_usd_amt"].round(
        NUMERIC_PRECISION
    )
    grouped["own_cd"] = grouped["own_code"]

    cols = [
        "year_num",
        "naics2_sector_cd",
        "state_cnty_fips_cd",
        "state_fips_cd",
        "cnty_fips_cd",
        "own_cd",
        "qcew_ann_avg_emp_lvl_num",
        "qcew_ttl_ann_wage_usd_amt",
        "qcew_avg_wkly_wage_usd_amt",
    ]
    return grouped[cols]


def process_year(year: int, raw_path: Path) -> pd.DataFrame:
    # Single-year helper that allows callers (including tests) to inject custom
    # paths without touching the batch runner and keeps file IO localized.
    if not raw_path.exists():
        raise FileNotFoundError(f"QCEW raw file not found: {raw_path}")
    raw = pd.read_csv(raw_path, dtype=str, low_memory=False)
    normalized = normalize_qcew_columns(raw)
    prepped = prepare_qcew_private(normalized, year=year)
    return prepped


def run_batch(
    years: list[int],
    raw_template: str,
    per_year_pattern: str,
    stacked_out: Optional[str],
    single_raw: Optional[str] = None,
) -> None:
    combined_frames: list[pd.DataFrame] = []
    for year in years:
        raw_path = (
            Path(single_raw)
            if single_raw and len(years) == 1
            else Path(raw_template.format(year=year))
        )
        print(f"[QCEW] Loading {raw_path} for {year}")
        yearly = process_year(year, raw_path)
        per_year_path = Path(per_year_pattern.format(year=year))
        per_year_path.parent.mkdir(parents=True, exist_ok=True)
        yearly.to_csv(per_year_path, index=False)
        print(f"[QCEW] Wrote {per_year_path} ({len(yearly):,} rows).")
        combined_frames.append(yearly)

    if stacked_out:
        # Stack + QA the multiyear output that feeds BigQuery. Duplicate keys
        # are treated as fatal because they would lead to silent overwrite on
        # load and undo the aggregation work above.
        combined = pd.concat(combined_frames, ignore_index=True)
        dupes = combined.duplicated(
            subset=["year_num", "state_cnty_fips_cd", "naics2_sector_cd"]
        ).sum()
        if dupes:
            raise AssertionError(f"Found {dupes} duplicate rows in combined QCEW output.")
        out_path = Path(stacked_out)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        combined.to_csv(out_path, index=False)
        print(f"[QCEW] Wrote combined dataset: {out_path} ({len(combined):,} rows).")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Prepare multi-year QCEW NAICS2 benchmarks.")
    parser.add_argument(
        "--qcew_raw",
        help="Path to a raw QCEW CSV (single-year shortcut; overrides template).",
    )
    parser.add_argument(
        "--year",
        type=int,
        help="Single year to run (default uses --years or MVP years).",
    )
    parser.add_argument(
        "--years",
        type=int,
        nargs="+",
        help="Explicit list of years to process.",
    )
    parser.add_argument(
        "--raw_template",
        default=DEFAULT_RAW_TEMPLATE,
        help="Template for raw QCEW files (use '{year}' placeholder).",
    )
    parser.add_argument(
        "--per_year_pattern",
        default=DEFAULT_PER_YEAR_PATTERN,
        help="Template for per-year outputs (use '{year}' placeholder).",
    )
    parser.add_argument(
        "--out",
        default=DEFAULT_STACKED_OUT,
        help="Combined multiyear output path (set empty to skip).",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if args.years:
        years = sorted(set(args.years))
    elif args.year:
        years = [args.year]
    else:
        years = MVP_YEARS.copy()
    run_batch(
        years=years,
        raw_template=args.raw_template,
        per_year_pattern=args.per_year_pattern,
        stacked_out=args.out,
        single_raw=args.qcew_raw,
    )


if __name__ == "__main__":
    main()
