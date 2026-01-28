#!/usr/bin/env python3
"""
QCEW Annual Averages Reconciliation QA
--------------------------------------
Purpose
  Validate RDM QCEW additive facts against BLS QCEW annual averages singlefile.

Data sources
  - BLS QCEW annual averages singlefile CSVs (local or downloaded)
  - RDM BigQuery table: rdm-datalab-portfolio.portfolio_data.econ_bnchmrk_abs_qcew

Tolerances
  - Employment and total annual wages must match exactly.
  - Avg weekly wage allows a $1 tolerance (rounding guard).
  - Suppressed/missing source rows are marked in notes and excluded from pass_all.

How to run
  python -m qa.qcew_reconciliation --years 2022 2023 --counties 06075 06085 \
      --naics 42 62 --outdir artifacts/qa --publish_bq false
  Optional: add --rdm_csv path/to/local_rdm.csv to bypass BigQuery.

Failure interpretation
  - pass_* flags indicate per-metric comparison success.
  - notes captures missing/suppressed source values or missing RDM rows.
"""

from __future__ import annotations

import argparse
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

import pandas as pd

from qa.utils import parse_bool, safe_divide


DEFAULT_COUNTIES = ["06075", "06085"]
DEFAULT_NAICS = ["42", "62"]
DEFAULT_YEARS = [2022, 2023]
DEFAULT_OUTDIR = "artifacts/qa"
DEFAULT_RAW_TEMPLATE = "data_raw/qcew/{year}.annual.singlefile.csv"
DEFAULT_CACHE_DIR = "data_raw/qcew/source_qa"

QCEW_TABLE = "rdm-datalab-portfolio.portfolio_data.econ_bnchmrk_abs_qcew"

SUPPRESSED_VALUES = {"", "D", "N", "S", "NA", "N/A", "(D)", "(N)", "(S)"}


@dataclass(frozen=True)
class QcewConfig:
    years: list[int]
    counties: list[str]
    naics: list[str]
    outdir: Path
    publish_bq: bool
    bq_table: str
    raw_template: str
    cache_dir: Path
    ownership_code: str
    agg_level: str
    allow_wage_tolerance: bool
    rdm_csv: Optional[Path]


def _parse_numeric(value: Any) -> tuple[Optional[float], Optional[str]]:
    if value is None:
        return None, "source_missing"
    text = str(value).strip()
    if text in SUPPRESSED_VALUES:
        return None, "source_suppressed"
    try:
        return float(text), None
    except ValueError:
        return None, "source_non_numeric"


def _normalize_naics2(code: str) -> Optional[str]:
    cleaned = "".join(ch for ch in str(code).strip() if ch.isdigit() or ch == "-")
    if not cleaned:
        return None
    if cleaned.startswith("31") or cleaned.startswith("32") or cleaned.startswith("33"):
        return "31-33"
    if cleaned.startswith("44") or cleaned.startswith("45"):
        return "44-45"
    if cleaned.startswith("48") or cleaned.startswith("49"):
        return "48-49"
    digits = "".join(ch for ch in cleaned if ch.isdigit())
    if len(digits) < 2:
        return None
    return digits[:2]


def _normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    rename_map = {}
    lower = {c.lower(): c for c in df.columns}

    def pick(*opts: str) -> Optional[str]:
        for opt in opts:
            if opt in lower:
                return lower[opt]
        return None

    area = pick("area_fips", "state_cnty_fips_cd", "fips")
    industry = pick("industry_code", "indstr_cd", "naics")
    year = pick("year", "year_num")
    emp = pick("annual_avg_emplvl", "annual_avg_emp", "qcew_ann_avg_emp_lvl_num")
    wages = pick("total_annual_wages", "qcew_ttl_ann_wage_usd_amt")
    avg_wage = pick("annual_avg_wkly_wage", "avg_weekly_wage", "qcew_avg_wkly_wage_usd_amt")
    own = pick("own_code", "ownership_code", "own")
    agglvl = pick("agglvl_code", "agg_lvl_cd", "aggregation_level")
    qtr = pick("qtr", "quarter")

    missing = [name for name, col in {
        "area_fips": area,
        "industry_code": industry,
        "year": year,
        "annual_avg_emplvl": emp,
        "total_annual_wages": wages,
        "annual_avg_wkly_wage": avg_wage,
        "agglvl_code": agglvl,
    }.items() if col is None]
    if missing:
        raise ValueError(f"QCEW source missing required columns: {missing}")

    rename_map.update({
        area: "area_fips",
        industry: "industry_code",
        year: "year",
        emp: "annual_avg_emplvl",
        wages: "total_annual_wages",
        avg_wage: "annual_avg_wkly_wage",
        agglvl: "agglvl_code",
    })
    if own:
        rename_map[own] = "own_code"
    if qtr:
        rename_map[qtr] = "qtr"
    return df.rename(columns=rename_map)


def load_qcew_source(config: QcewConfig) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    for year in config.years:
        raw_path = Path(config.raw_template.format(year=year))
        if not raw_path.exists():
            cached = config.cache_dir / f"{year}.annual.singlefile.csv"
            if cached.exists():
                raw_path = cached
            else:
                raise FileNotFoundError(
                    f"QCEW source file not found for {year}. Expected {raw_path} or {cached}."
                )
        raw = pd.read_csv(raw_path, dtype=str, low_memory=False)
        normalized = _normalize_columns(raw)
        normalized["year"] = normalized["year"].astype(str)
        normalized = normalized[normalized["year"] == str(year)].copy()
        if "qtr" in normalized.columns:
            normalized = normalized[normalized["qtr"].astype(str).str.upper() == "A"].copy()
        if "own_code" in normalized.columns:
            normalized = normalized[normalized["own_code"].astype(str) == str(config.ownership_code)].copy()
        else:
            normalized["own_code"] = str(config.ownership_code)
        normalized = normalized[normalized["agglvl_code"].astype(str) == str(config.agg_level)].copy()

        normalized["area_fips"] = normalized["area_fips"].astype(str).str.zfill(5)
        normalized = normalized[normalized["area_fips"].str.len() == 5].copy()
        normalized["state_cnty_fips_cd"] = normalized["area_fips"]
        normalized["state_fips"] = normalized["area_fips"].str[:2]
        normalized["county_fips"] = normalized["area_fips"].str[2:]
        normalized["naics2_sector_cd"] = normalized["industry_code"].apply(_normalize_naics2)
        normalized = normalized[normalized["naics2_sector_cd"].notna()].copy()

        frames.append(normalized)

    combined = pd.concat(frames, ignore_index=True)
    combined = combined[combined["state_cnty_fips_cd"].isin(config.counties)].copy()
    combined = combined[combined["naics2_sector_cd"].isin(config.naics)].copy()
    combined = combined.drop_duplicates(
        subset=["year", "state_cnty_fips_cd", "naics2_sector_cd"]
    )
    return combined


def fetch_rdm_qcew(
    years: list[int],
    counties: list[str],
    naics: list[str],
    rdm_csv: Optional[Path] = None,
) -> pd.DataFrame:
    if rdm_csv:
        df = pd.read_csv(rdm_csv, dtype=str)
        required = {
            "year_num",
            "state_cnty_fips_cd",
            "naics2_sector_cd",
            "qcew_ann_avg_emp_lvl_num",
            "qcew_ttl_ann_wage_usd_amt",
        }
        missing = required - set(df.columns)
        if missing:
            raise ValueError(f"RDM CSV missing required columns: {sorted(missing)}")
        df["state_cnty_fips_cd"] = df["state_cnty_fips_cd"].astype(str).str.zfill(5)
        df["naics2_sector_cd"] = df["naics2_sector_cd"].astype(str).str.zfill(2)
        df["year_num"] = pd.to_numeric(df["year_num"], errors="coerce")
        df = df[df["year_num"].isin(years)]
        df = df[df["state_cnty_fips_cd"].isin([str(c).zfill(5) for c in counties])]
        df = df[df["naics2_sector_cd"].isin([str(n).zfill(2) for n in naics])]
        df = df.rename(
            columns={
                "qcew_ann_avg_emp_lvl_num": "rdm_qcew_emp",
                "qcew_ttl_ann_wage_usd_amt": "rdm_qcew_wages_usd",
                "qcew_avg_wkly_wage_usd_amt": "rdm_qcew_avg_weekly_wage_usd",
            }
        )
        if "rdm_qcew_avg_weekly_wage_usd" not in df.columns:
            df["rdm_qcew_avg_weekly_wage_usd"] = pd.NA
        return df[
            [
                "year_num",
                "state_cnty_fips_cd",
                "naics2_sector_cd",
                "rdm_qcew_emp",
                "rdm_qcew_wages_usd",
                "rdm_qcew_avg_weekly_wage_usd",
            ]
        ]
    try:
        from google.cloud import bigquery
    except ImportError as exc:
        raise RuntimeError("google-cloud-bigquery is required to fetch RDM data") from exc

    client = bigquery.Client()
    query = f"""
        SELECT
            year_num,
            state_cnty_fips_cd,
            naics2_sector_cd,
            qcew_ann_avg_emp_lvl_num AS rdm_qcew_emp,
            qcew_ttl_ann_wage_usd_amt AS rdm_qcew_wages_usd,
            qcew_avg_wkly_wage_usd_amt AS rdm_qcew_avg_weekly_wage_usd
        FROM `{QCEW_TABLE}`
        WHERE year_num IN UNNEST(@years)
          AND state_cnty_fips_cd IN UNNEST(@counties)
          AND naics2_sector_cd IN UNNEST(@naics)
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ArrayQueryParameter("years", "INT64", years),
            bigquery.ArrayQueryParameter("counties", "STRING", [str(c).zfill(5) for c in counties]),
            bigquery.ArrayQueryParameter("naics", "STRING", [str(n).zfill(2) for n in naics]),
        ]
    )
    df = client.query(query, job_config=job_config).to_dataframe()
    df["state_cnty_fips_cd"] = df["state_cnty_fips_cd"].astype(str).str.zfill(5)
    df["naics2_sector_cd"] = df["naics2_sector_cd"].astype(str).str.zfill(2)
    return df


def reconcile_qcew(source_df: pd.DataFrame, rdm_df: pd.DataFrame, allow_wage_tolerance: bool) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for _, row in source_df.iterrows():
        notes: list[str] = []
        emp, emp_note = _parse_numeric(row.get("annual_avg_emplvl"))
        if emp_note:
            notes.append(emp_note)
        wages, wages_note = _parse_numeric(row.get("total_annual_wages"))
        if wages_note:
            notes.append(wages_note)
        avg_wage, avg_note = _parse_numeric(row.get("annual_avg_wkly_wage"))
        if avg_note:
            notes.append(avg_note)

        rows.append(
            {
                "year_num": int(row["year"]),
                "state_cnty_fips_cd": row["state_cnty_fips_cd"],
                "state_fips": row["state_fips"],
                "county_fips": row["county_fips"],
                "naics2_sector_cd": row["naics2_sector_cd"],
                "source_qcew_annual_avg_emplvl": emp,
                "source_qcew_total_annual_wages_usd": wages,
                "source_qcew_avg_weekly_wage_usd": avg_wage,
                "notes": ";".join(sorted(set(notes))) if notes else "",
            }
        )

    source_clean = pd.DataFrame(rows)
    merged = source_clean.merge(
        rdm_df,
        how="outer",
        on=["year_num", "state_cnty_fips_cd", "naics2_sector_cd"],
        suffixes=("", "_rdm"),
    )
    merged["state_cnty_fips_cd"] = merged["state_cnty_fips_cd"].astype(str).str.zfill(5)
    merged["state_fips"] = merged["state_cnty_fips_cd"].str[:2]
    merged["county_fips"] = merged["state_cnty_fips_cd"].str[2:]
    numeric_cols = [
        "source_qcew_annual_avg_emplvl",
        "source_qcew_total_annual_wages_usd",
        "source_qcew_avg_weekly_wage_usd",
        "rdm_qcew_emp",
        "rdm_qcew_wages_usd",
        "rdm_qcew_avg_weekly_wage_usd",
    ]
    for col in numeric_cols:
        merged[col] = pd.to_numeric(merged[col], errors="coerce")

    fallback_wage = merged["rdm_qcew_wages_usd"] / (merged["rdm_qcew_emp"] * 52.0)
    fallback_wage = fallback_wage.where(merged["rdm_qcew_emp"] > 0)
    merged["rdm_qcew_avg_weekly_wage_usd"] = merged["rdm_qcew_avg_weekly_wage_usd"].where(
        merged["rdm_qcew_avg_weekly_wage_usd"].notna(),
        fallback_wage,
    )

    merged["delta_emp"] = merged["rdm_qcew_emp"] - merged["source_qcew_annual_avg_emplvl"]
    merged["delta_wages_usd"] = merged["rdm_qcew_wages_usd"] - merged["source_qcew_total_annual_wages_usd"]
    merged["delta_avg_weekly_wage_usd"] = (
        merged["rdm_qcew_avg_weekly_wage_usd"] - merged["source_qcew_avg_weekly_wage_usd"]
    )

    merged["delta_emp_pct"] = merged.apply(
        lambda r: safe_divide(r["delta_emp"], r["source_qcew_annual_avg_emplvl"]), axis=1
    )
    merged["delta_wages_pct"] = merged.apply(
        lambda r: safe_divide(r["delta_wages_usd"], r["source_qcew_total_annual_wages_usd"]), axis=1
    )
    merged["delta_avg_weekly_wage_pct"] = merged.apply(
        lambda r: safe_divide(r["delta_avg_weekly_wage_usd"], r["source_qcew_avg_weekly_wage_usd"]), axis=1
    )

    def _pass_exact(delta: Any, left: Any, right: Any) -> Optional[bool]:
        if pd.isna(left) or pd.isna(right):
            return None
        return delta == 0

    def _pass_tol(delta: Any, left: Any, right: Any, tol: float) -> Optional[bool]:
        if pd.isna(left) or pd.isna(right):
            return None
        return abs(delta) <= tol

    merged["pass_emp"] = merged.apply(
        lambda r: _pass_exact(r["delta_emp"], r["rdm_qcew_emp"], r["source_qcew_annual_avg_emplvl"]),
        axis=1,
    )
    merged["pass_wages"] = merged.apply(
        lambda r: _pass_exact(r["delta_wages_usd"], r["rdm_qcew_wages_usd"], r["source_qcew_total_annual_wages_usd"]),
        axis=1,
    )
    merged["pass_avg_weekly_wage"] = merged.apply(
        lambda r: _pass_tol(r["delta_avg_weekly_wage_usd"], r["rdm_qcew_avg_weekly_wage_usd"], r["source_qcew_avg_weekly_wage_usd"], 1.0)
        if allow_wage_tolerance
        else _pass_exact(r["delta_avg_weekly_wage_usd"], r["rdm_qcew_avg_weekly_wage_usd"], r["source_qcew_avg_weekly_wage_usd"]),
        axis=1,
    )

    pass_all = []
    notes = []
    for _, row in merged.iterrows():
        row_notes = row.get("notes", "")
        flags = [row_notes] if row_notes else []
        if pd.isna(row["source_qcew_annual_avg_emplvl"]) and pd.isna(row["rdm_qcew_emp"]):
            flags.append("missing_from_both")
            pass_all.append(None)
        elif pd.isna(row["source_qcew_annual_avg_emplvl"]):
            flags.append("missing_from_source")
            pass_all.append(None)
        elif pd.isna(row["rdm_qcew_emp"]):
            flags.append("missing_from_rdm")
            pass_all.append(False)
        else:
            if row["pass_emp"] is True and row["pass_wages"] is True:
                pass_all.append(True)
            elif row["pass_emp"] is None or row["pass_wages"] is None:
                pass_all.append(None)
            else:
                pass_all.append(False)
        notes.append(";".join([flag for flag in flags if flag]))

    merged["pass_all"] = pass_all
    merged["notes"] = notes

    keep = [
        "year_num",
        "state_cnty_fips_cd",
        "state_fips",
        "county_fips",
        "naics2_sector_cd",
        "source_qcew_annual_avg_emplvl",
        "source_qcew_total_annual_wages_usd",
        "source_qcew_avg_weekly_wage_usd",
        "rdm_qcew_emp",
        "rdm_qcew_wages_usd",
        "rdm_qcew_avg_weekly_wage_usd",
        "delta_emp",
        "delta_wages_usd",
        "delta_avg_weekly_wage_usd",
        "delta_emp_pct",
        "delta_wages_pct",
        "delta_avg_weekly_wage_pct",
        "pass_emp",
        "pass_wages",
        "pass_avg_weekly_wage",
        "pass_all",
        "notes",
    ]
    return merged[keep]


def write_outputs(df: pd.DataFrame, outdir: Path, publish_bq: bool, bq_table: str) -> tuple[Path, Path]:
    outdir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    out_path = outdir / f"qcew_reconciliation_{timestamp}.csv"
    latest_path = outdir / "qcew_reconciliation_latest.csv"
    df.to_csv(out_path, index=False)
    df.to_csv(latest_path, index=False)

    if publish_bq:
        try:
            from google.cloud import bigquery
        except ImportError as exc:
            raise RuntimeError("google-cloud-bigquery is required to publish results") from exc

        client = bigquery.Client()
        run_id = str(uuid.uuid4())
        run_ts = datetime.now(timezone.utc)
        load_df = df.copy()
        load_df["run_id"] = run_id
        load_df["run_ts"] = run_ts
        job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
        client.load_table_from_dataframe(load_df, bq_table, job_config=job_config).result()
    return out_path, latest_path


def parse_args(argv: Optional[list[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="QCEW annual averages reconciliation QA.")
    parser.add_argument("--years", nargs="+", type=int, default=DEFAULT_YEARS)
    parser.add_argument("--counties", nargs="+", default=DEFAULT_COUNTIES)
    parser.add_argument("--naics", nargs="+", default=DEFAULT_NAICS)
    parser.add_argument("--outdir", default=DEFAULT_OUTDIR)
    parser.add_argument("--publish_bq", default="false")
    parser.add_argument("--bq_table", default="rdm-datalab-portfolio.portfolio_data.qa_qcew_reconciliation")
    parser.add_argument("--raw_template", default=DEFAULT_RAW_TEMPLATE)
    parser.add_argument("--cache_dir", default=DEFAULT_CACHE_DIR)
    parser.add_argument("--ownership_code", default="5")
    parser.add_argument("--agg_level", default="74")
    parser.add_argument("--allow_wage_tolerance", default="true")
    parser.add_argument("--rdm_csv", default=None)
    return parser.parse_args(argv)


def run(config: QcewConfig) -> pd.DataFrame:
    source_df = load_qcew_source(config)
    rdm_df = fetch_rdm_qcew(config.years, config.counties, config.naics, config.rdm_csv)
    return reconcile_qcew(source_df, rdm_df, config.allow_wage_tolerance)


def main(argv: Optional[list[str]] = None) -> None:
    args = parse_args(argv)
    config = QcewConfig(
        years=args.years,
        counties=[str(c).zfill(5) for c in args.counties],
        naics=[str(n).zfill(2) for n in args.naics],
        outdir=Path(args.outdir),
        publish_bq=parse_bool(args.publish_bq),
        bq_table=args.bq_table,
        raw_template=args.raw_template,
        cache_dir=Path(args.cache_dir),
        ownership_code=str(args.ownership_code),
        agg_level=str(args.agg_level),
        allow_wage_tolerance=parse_bool(args.allow_wage_tolerance, default=True),
        rdm_csv=Path(args.rdm_csv) if args.rdm_csv else None,
    )
    df = run(config)
    out_path, latest_path = write_outputs(df, config.outdir, config.publish_bq, config.bq_table)
    total = len(df)
    passed = int((df["pass_all"] == True).sum()) if total else 0
    failures = df[df["pass_all"] == False]
    print(f"[QCEW] Wrote {out_path} and {latest_path}")
    print(f"[QCEW] pass_all: {passed}/{total}")
    if not failures.empty:
        print("[QCEW] Failures:")
        for _, row in failures.iterrows():
            print(
                f"  - {row['year_num']} {row['state_cnty_fips_cd']} {row['naics2_sector_cd']}: "
                f"emp={row['pass_emp']} wages={row['pass_wages']} avg_weekly_wage={row['pass_avg_weekly_wage']}"
            )


if __name__ == "__main__":
    main()
