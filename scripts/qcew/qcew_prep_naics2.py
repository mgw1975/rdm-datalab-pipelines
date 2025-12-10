# qcew_prep_naics2.py
# -------------------------------------------------------------
# Prepare BLS QCEW wages for portfolio use at:
#   county × 2-digit NAICS × year
#
# The script now supports multi-year processing. Provide either:
#   - --qcew_raw, --year, --out  (single year, backward-compatible)
#   - or --years plus --raw_template/--per_year_pattern for batch runs.
#
# MVP default: a post-pandemic panel for 2022–2023. Additional years can be
# added later, but anything earlier than 2022 should live in a separate
# "historical" project. The default raw template expects BLS' annual
# single-file naming pattern of "{year}.annual.singlefile.csv".
#
from __future__ import annotations

import argparse
from pathlib import Path

import numpy as np
import pandas as pd

NUMERIC_PRECISION = 9  # BigQuery NUMERIC supports up to 9 decimal places

def normalize_qcew_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Standardize flexible column names in a raw QCEW dump."""
    lower = {c.lower(): c for c in df.columns}

    def pick(*opts):
        for opt in opts:
            if opt in lower:
                return lower[opt]
        return None

    area = pick("area_fips", "area", "fips")
    ind = pick("industry_code", "naics", "industry")
    year_col = pick("year")
    aemp = pick(
        "annual_avg_emplvl",
        "annual_avg_employment",
        "annualaverageemployment",
        "annual_avg_emplv",
    )
    twages = pick(
        "total_annual_wages",
        "totalannualwages",
        "annual_total_wages",
        "tot_annual_wages",
    )
    awage = pick(
        "avg_wkly_wage",
        "avg_weekly_wage",
        "average_weekly_wage",
        "annual_avg_wkly_wage",
    )
    own = pick("own_code", "ownership", "own")

    need = [area, ind, year_col, aemp, twages, awage]
    if any(x is None for x in need):
        missing = [
            n
            for n, x in zip(
                [
                    "area_fips",
                    "industry_code",
                    "year",
                    "annual_avg_emplvl",
                    "total_annual_wages",
                    "avg_weekly_wage",
                ],
                need,
            )
            if x is None
        ]
        raise ValueError(
            f"QCEW file missing required columns (or synonyms): {missing}"
        )

    df = df.rename(
        columns={
            area: "area_fips",
            ind: "industry_code",
            year_col: "year",
            aemp: "annual_avg_emplvl",
            twages: "total_annual_wages",
            awage: "avg_weekly_wage",
        }
    )
    if own:
        df = df.rename(columns={own: "own_code"})
    return df


def prepare_qcew_naics2(
    qdf: pd.DataFrame, year: int | None = None, keep_own_code_zero: bool = True
) -> pd.DataFrame:
    """Filter, aggregate, and derive NAICS2-level QCEW metrics."""
    df = qdf.copy()
    if year is not None and "year" in df.columns:
        df = df[df["year"].astype(str) == str(year)]
    if keep_own_code_zero and "own_code" in df.columns:
        df = df[df["own_code"].astype(str) == "0"]

    df["area_fips"] = df["area_fips"].astype(str).str.strip()
    df = df[df["area_fips"].str.len() == 5]
    df["state_fips"] = df["area_fips"].str[:2]
    df["county_fips"] = df["area_fips"].str[2:]

    df["naics2"] = df["industry_code"].astype(str).str.extract(r"(\d+)", expand=False)
    df["naics2"] = df["naics2"].str[:2]
    df = df[df["naics2"].str.len() == 2]

    for col in ["annual_avg_emplvl", "total_annual_wages", "avg_weekly_wage"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    if "year" in df.columns:
        df["year"] = pd.to_numeric(df["year"], errors="coerce")

    grp = (
        df.groupby(["state_fips", "county_fips", "naics2", "year"], as_index=False)
        .agg({
            "annual_avg_emplvl": "sum",
            "total_annual_wages": "sum"
        })
    )
    grp["avg_weekly_wage"] = np.where(
        grp["annual_avg_emplvl"] > 0,
        grp["total_annual_wages"] / (grp["annual_avg_emplvl"] * 52.0),
        np.nan,
    )
    out = grp[
        [
            "state_fips",
            "county_fips",
            "naics2",
            "year",
            "annual_avg_emplvl",
            "total_annual_wages",
            "avg_weekly_wage",
        ]
    ]
    out["avg_weekly_wage"] = out["avg_weekly_wage"].round(NUMERIC_PRECISION)
    return out


def finalize_qcew(df: pd.DataFrame) -> pd.DataFrame:
    """Align QCEW columns to the canonical schema."""
    out = df.rename(
        columns={
            "state_fips": "state_fips_cd",
            "county_fips": "cnty_fips_cd",
            "naics2": "naics2_sector_cd",
            "year": "year_num",
            "annual_avg_emplvl": "qcew_ann_avg_emp_lvl_num",
            "total_annual_wages": "qcew_ttl_ann_wage_usd_amt",
            "avg_weekly_wage": "qcew_avg_wkly_wage_usd_amt",
        }
    )
    out["state_cnty_fips_cd"] = (
        out["state_fips_cd"].astype(str).str.zfill(2)
        + out["cnty_fips_cd"].astype(str).str.zfill(3)
    )
    cols = [
        "year_num",
        "state_cnty_fips_cd",
        "state_fips_cd",
        "cnty_fips_cd",
        "naics2_sector_cd",
        "qcew_ann_avg_emp_lvl_num",
        "qcew_ttl_ann_wage_usd_amt",
        "qcew_avg_wkly_wage_usd_amt",
    ]
    return out[cols]


DEFAULT_RAW_TEMPLATE = "data_raw/qcew/{year}.annual.singlefile.csv"
DEFAULT_PER_YEAR_PATTERN = "data_clean/qcew/econ_bnchmrk_qcew_{year}.csv"
DEFAULT_STACKED_OUT = "data_clean/qcew/econ_bnchmrk_qcew_multiyear.csv"
MVP_YEARS = [2022, 2023]


def run_batch(
    years: list[int],
    raw_template: str,
    per_year_pattern: str,
    stacked_out: str | None,
    single_raw: str | None = None,
) -> None:
    """Process multiple QCEW years using provided templates."""
    stacked_frames: list[pd.DataFrame] = []
    for year in years:
        raw_path = (
            Path(single_raw)
            if single_raw and len(years) == 1
            else Path(raw_template.format(year=year))
        )
        if not raw_path.exists():
            raise FileNotFoundError(f"QCEW raw file not found: {raw_path}")
        print(f"[QCEW] Loading raw file for {year}: {raw_path}")
        raw = pd.read_csv(raw_path, dtype=str)
        raw = normalize_qcew_columns(raw)
        prepped = prepare_qcew_naics2(raw, year=year, keep_own_code_zero=True)
        finalized = finalize_qcew(prepped)

        per_year_path = Path(per_year_pattern.format(year=year))
        per_year_path.parent.mkdir(parents=True, exist_ok=True)
        finalized.to_csv(per_year_path, index=False)
        print(f"[QCEW] Wrote {per_year_path} ({len(finalized):,} rows).")
        stacked_frames.append(finalized)

    if stacked_out:
        combined = pd.concat(stacked_frames, ignore_index=True)
        dupes = combined.duplicated(
            subset=["year_num", "state_cnty_fips_cd", "naics2_sector_cd"]
        ).sum()
        if dupes:
            raise AssertionError(
                f"Found {dupes} duplicate rows in combined QCEW output."
            )
        out_path = Path(stacked_out)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        combined.to_csv(out_path, index=False)
        print(f"[QCEW] Wrote combined dataset: {out_path} ({len(combined):,} rows).")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--qcew_raw",
        help="Path to a raw QCEW CSV (single-year mode).",
    )
    parser.add_argument(
        "--year",
        type=int,
        default=None,
        help="Optional single year (default: MVP years).",
    )
    parser.add_argument(
        "--years",
        type=int,
        nargs="+",
        help="Optional list of years to process (default: use --year).",
    )
    parser.add_argument(
        "--raw_template",
        default=DEFAULT_RAW_TEMPLATE,
        help="Pattern for raw QCEW files (use '{year}' placeholder).",
    )
    parser.add_argument(
        "--per_year_pattern",
        default=DEFAULT_PER_YEAR_PATTERN,
        help="Pattern for per-year outputs (use '{year}' placeholder).",
    )
    parser.add_argument(
        "--out",
        default=DEFAULT_STACKED_OUT,
        help="Combined multiyear output path (default: %(default)s).",
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
