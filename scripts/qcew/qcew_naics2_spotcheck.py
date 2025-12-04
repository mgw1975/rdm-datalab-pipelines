#!/usr/bin/env python3
"""
Debug helper: compare the current qcew_prep_naics_sector aggregation against the
official BLS QCEW two-digit NAICS rows for a single county.

Example:
    python scripts/qcew/qcew_naics2_spotcheck.py \
        --qcew_raw data_raw/qcew/2022.annual.singlefile.csv \
        --year 2022 \
        --area 06087
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.append(str(REPO_ROOT))

from scripts.qcew.qcew_prep_naics_sector import (  # type: ignore # noqa: E402
    normalize_qcew_columns,
    prepare_qcew_sector,
)


def load_normalized(raw_path: Path) -> pd.DataFrame:
    df = pd.read_csv(raw_path, dtype=str, low_memory=False)
    lower = {c.lower(): c for c in df.columns}
    weekly_col = lower.get("avg_wkly_wage") or lower.get("avg_weekly_wage")
    if not weekly_col:
        annual_weekly = lower.get("annual_avg_wkly_wage")
        if annual_weekly:
            df["avg_wkly_wage"] = df[annual_weekly]
    lower = {c.lower(): c for c in df.columns}
    if "avg_weekly_wage" not in lower and "avg_wkly_wage" not in lower:
        raise ValueError(
            "QCEW file missing avg weekly wage column; "
            "add a synonym mapping in qcew_naics2_spotcheck.py"
        )
    df = normalize_qcew_columns(df)
    return df


def run_pipeline(
    df: pd.DataFrame, year: int, area: str, agg_filter: str | None
) -> pd.DataFrame:
    """Run the existing prep logic and filter down to one county."""
    working = df.copy()
    if agg_filter and "agg_lvl_cd" in working.columns:
        working = working[working["agg_lvl_cd"].astype(str) == agg_filter]
    prep = prepare_qcew_sector(
        working, year=year, prefer_private_if_total_missing=True
    )
    prep["state_cnty_fips_cd"] = prep["state_cnty_fips_cd"].astype(str).str.zfill(5)
    spot = prep[prep["state_cnty_fips_cd"] == area].copy()
    spot["naics2"] = spot["naics2_sector_cd"].astype(str)
    spot["annual_avg_emplvl"] = pd.to_numeric(
        spot["qcew_ann_avg_emp_lvl_num"], errors="coerce"
    )
    spot["avg_weekly_wage"] = pd.to_numeric(
        spot["qcew_avg_wkly_wage_usd_amt"], errors="coerce"
    )
    spot = spot.rename(
        columns={
            "qcew_ttl_ann_wage_usd_amt": "total_annual_wages",
        }
    )
    return spot[
        ["naics2", "annual_avg_emplvl", "avg_weekly_wage", "total_annual_wages"]
    ].reset_index(drop=True)


def pull_bls_reference(df: pd.DataFrame, year: int, area: str) -> pd.DataFrame:
    """Grab the official county×NAICS2 rows (agglvl_code 74, private ownership)."""
    ref = df.copy()
    year_col = "year" if "year" in ref.columns else "year_num"
    area_col = (
        "area_fips"
        if "area_fips" in ref.columns
        else "state_cnty_fips_cd"
    )
    ref = ref[ref[year_col].astype(str) == str(year)]
    ref = ref[ref[area_col].astype(str) == area]
    # Private ownership (5) because that is what most public tables use.
    ref = ref[ref.get("own_code", "").astype(str) == "5"]
    # County × NAICS sector = agglvl 74 according to the QCEW layout file.
    agg_col = "agglvl_code" if "agglvl_code" in ref.columns else "agg_lvl_cd"
    if agg_col in ref.columns:
        ref = ref[ref[agg_col].astype(str) == "74"]
    ind_col = "industry_code" if "industry_code" in ref.columns else "indstr_cd"
    ref["naics2"] = ref[ind_col].astype(str).str.extract(r"^(\d{2})", expand=False)
    col_map = {
        "annual_avg_emplvl": "qcew_ann_avg_emp_lvl_num",
        "avg_weekly_wage": "qcew_avg_wkly_wage_usd_amt",
        "total_annual_wages": "qcew_ttl_ann_wage_usd_amt",
        "avg_annual_pay": "avg_annual_pay",
    }
    for friendly, actual in col_map.items():
        if actual in ref.columns:
            ref[actual] = pd.to_numeric(ref[actual], errors="coerce")
        else:
            ref[actual] = pd.NA
    ref = ref.rename(
        columns={
            "qcew_ann_avg_emp_lvl_num": "annual_avg_emplvl",
            "qcew_avg_wkly_wage_usd_amt": "avg_weekly_wage",
            "qcew_ttl_ann_wage_usd_amt": "total_annual_wages",
        }
    )
    ref = ref[
        ["naics2", "annual_avg_emplvl", "avg_weekly_wage", "avg_annual_pay", "total_annual_wages"]
    ].reset_index(drop=True)
    return ref


def build_comparison(pipeline: pd.DataFrame, bls: pd.DataFrame) -> pd.DataFrame:
    merged = pd.merge(
        pipeline,
        bls,
        on="naics2",
        how="outer",
        suffixes=("_pipeline", "_bls"),
    ).sort_values("naics2")
    merged["employment_diff"] = (
        merged["annual_avg_emplvl_pipeline"] - merged["annual_avg_emplvl_bls"]
    )
    merged["wage_diff"] = (
        merged["avg_weekly_wage_pipeline"] - merged["avg_weekly_wage_bls"]
    )
    return merged


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(
        description="Spot-check how qcew_prep_naics2 aggregates a single county."
    )
    ap.add_argument("--qcew_raw", required=True, help="Path to raw QCEW CSV.")
    ap.add_argument("--year", type=int, default=2022, help="Year to analyze.")
    ap.add_argument(
        "--area",
        default="06087",
        help="5-digit county FIPS to inspect (default: 06087 Santa Cruz).",
    )
    ap.add_argument(
        "--out",
        default="docs/qcew_county_spotcheck.csv",
        help="Optional CSV path for the merged comparison.",
    )
    ap.add_argument(
        "--agg_filter",
        default=None,
        help="Optional aggregation-level code to filter before running the pipeline "
        "(e.g., 74 to limit to county×sector rows). Leave blank to mimic current behavior.",
    )
    return ap.parse_args()


def main() -> None:
    args = parse_args()
    raw = load_normalized(Path(args.qcew_raw))
    pipeline = run_pipeline(raw, year=args.year, area=args.area, agg_filter=args.agg_filter)
    bls = pull_bls_reference(raw, year=args.year, area=args.area)
    merged = build_comparison(pipeline, bls)
    merged.to_csv(args.out, index=False)
    print(
        f"Saved comparison for county {args.area} ({len(merged)} rows) to {args.out}."
    )
    biggest = merged.loc[
        merged["employment_diff"].abs().nlargest(5).index,
        [
            "naics2",
            "annual_avg_emplvl_pipeline",
            "annual_avg_emplvl_bls",
            "employment_diff",
        ],
    ]
    print("Top employment deltas:")
    print(biggest.to_string(index=False))


if __name__ == "__main__":
    main()
