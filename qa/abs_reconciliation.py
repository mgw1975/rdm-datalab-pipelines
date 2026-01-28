#!/usr/bin/env python3
"""
ABS Census API Reconciliation QA
--------------------------------
Purpose
  Validate RDM ABS additive facts against Census ABS County API for the same
  county × NAICS2 × year slices.

Data sources
  - Census ABS County API: https://api.census.gov/data/{year}/abscs
  - RDM BigQuery table: rdm-datalab-portfolio.portfolio_data.econ_bnchmrk_abs_qcew

Tolerances
  - Firms and employment must match exactly.
  - Payroll and receipts must match after scaling to USD, with <= $1,000 slack.

How to run
  python -m qa.abs_reconciliation --years 2022 2023 --counties 06075 06085 \
      --naics 42 62 --outdir artifacts/qa --publish_bq false
  Optional: add --rdm_csv path/to/local_rdm.csv to bypass BigQuery.

Failure interpretation
  - pass_* flags indicate per-metric comparison success.
  - notes captures missing/suppressed source values or missing RDM rows.
"""

from __future__ import annotations

import argparse
import json
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional
from urllib.parse import urlencode
from urllib.request import urlopen

import pandas as pd

from qa.utils import parse_bool, safe_divide


CENSUS_BASE_URL = "https://api.census.gov/data/{year}/abscs"
CENSUS_GET = "NAICS2022,NAME,FIRMPDEMP,EMP,PAYANN,RCPPDEMP"
SUPPRESSED_VALUES = {"", "D", "N", "S", "NA", "N/A", "(D)", "(N)", "(S)"}

DEFAULT_COUNTIES = ["06075", "06085"]
DEFAULT_NAICS = ["42", "62"]
DEFAULT_YEARS = [2022, 2023]
DEFAULT_OUTDIR = "artifacts/qa"

ABS_TABLE = "rdm-datalab-portfolio.portfolio_data.econ_bnchmrk_abs_qcew"

# US states + DC, used for state-level ABS bulk pulls.
STATE_FIPS = [
    "01",
    "02",
    "04",
    "05",
    "06",
    "08",
    "09",
    "10",
    "11",
    "12",
    "13",
    "15",
    "16",
    "17",
    "18",
    "19",
    "20",
    "21",
    "22",
    "23",
    "24",
    "25",
    "26",
    "27",
    "28",
    "29",
    "30",
    "31",
    "32",
    "33",
    "34",
    "35",
    "36",
    "37",
    "38",
    "39",
    "40",
    "41",
    "42",
    "44",
    "45",
    "46",
    "47",
    "48",
    "49",
    "50",
    "51",
    "53",
    "54",
    "55",
    "56",
]


@dataclass(frozen=True)
class AbsConfig:
    years: list[int]
    counties: list[str]
    naics: list[str]
    outdir: Path
    publish_bq: bool
    bq_table: str
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


def parse_census_payload(payload: str) -> dict[str, Any]:
    try:
        data = json.loads(payload)
    except json.JSONDecodeError as exc:
        return {"notes": f"census_json_error:{exc}"}

    if not data or len(data) < 2:
        return {"notes": "census_empty_response"}

    header = data[0]
    row = data[1]
    return dict(zip(header, row))


def _fetch_census_slice(year: int, state_fips: str, county_fips: str, naics2: str) -> dict[str, Any]:
    params = {
        "get": CENSUS_GET,
        "for": f"county:{county_fips}",
        "in": f"state:{state_fips}",
        "NAICS2022": naics2,
    }
    url = f"{CENSUS_BASE_URL.format(year=year)}?{urlencode(params)}"
    try:
        with urlopen(url, timeout=30) as resp:
            payload = resp.read().decode("utf-8")
    except Exception as exc:
        return {"notes": f"census_http_error:{exc}"}

    return parse_census_payload(payload)


def fetch_census_data(years: list[int], counties: list[str], naics: list[str]) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for year in years:
        for county in counties:
            county = str(county).zfill(5)
            state_fips = county[:2]
            county_fips = county[2:]
            for naics2 in naics:
                record = _fetch_census_slice(year, state_fips, county_fips, str(naics2).zfill(2))
                notes: list[str] = []
                if "notes" in record:
                    notes.append(record["notes"])
                firm, note = _parse_numeric(record.get("FIRMPDEMP"))
                if note:
                    notes.append(note)
                emp, note = _parse_numeric(record.get("EMP"))
                if note:
                    notes.append(note)
                payann, note = _parse_numeric(record.get("PAYANN"))
                if note:
                    notes.append(note)
                rcpt, note = _parse_numeric(record.get("RCPPDEMP"))
                if note:
                    notes.append(note)

                rows.append(
                    {
                        "year_num": year,
                        "state_cnty_fips_cd": county,
                        "state_fips": state_fips,
                        "county_fips": county_fips,
                        "naics2_sector_cd": str(naics2).zfill(2),
                        "source_census_firmpdemp": firm,
                        "source_census_emp": emp,
                        "source_census_payann_usd": payann * 1000 if payann is not None else None,
                        "source_census_rcppdemp_usd": rcpt * 1000 if rcpt is not None else None,
                        "notes": ";".join(sorted(set(notes))) if notes else "",
                    }
                )
    return pd.DataFrame(rows)


def fetch_census_data_states(years: list[int], states: Optional[list[str]] = None) -> pd.DataFrame:
    """Bulk-pull ABS by state to cover all counties × NAICS2 for each year."""
    rows: list[dict[str, Any]] = []
    target_states = states or STATE_FIPS
    for year in years:
        for state in target_states:
            params = {
                "get": CENSUS_GET,
                "for": "county:*",
                "in": f"state:{state}",
            }
            url = f"{CENSUS_BASE_URL.format(year=year)}?{urlencode(params)}"
            try:
                with urlopen(url, timeout=60) as resp:
                    payload = resp.read().decode("utf-8")
            except Exception as exc:
                rows.append(
                    {
                        "year_num": year,
                        "state_cnty_fips_cd": f"{state}000",
                        "naics2_sector_cd": "",
                        "source_census_firmpdemp": None,
                        "source_census_emp": None,
                        "source_census_payann_usd": None,
                        "source_census_rcppdemp_usd": None,
                        "notes": f"census_http_error:{exc}",
                    }
                )
                continue

            try:
                data = json.loads(payload)
            except json.JSONDecodeError as exc:
                rows.append(
                    {
                        "year_num": year,
                        "state_cnty_fips_cd": f"{state}000",
                        "naics2_sector_cd": "",
                        "source_census_firmpdemp": None,
                        "source_census_emp": None,
                        "source_census_payann_usd": None,
                        "source_census_rcppdemp_usd": None,
                        "notes": f"census_json_error:{exc}",
                    }
                )
                continue

            if not data or len(data) < 2:
                continue

            header = data[0]
            for row in data[1:]:
                record = dict(zip(header, row))
                notes: list[str] = []
                firm, note = _parse_numeric(record.get("FIRMPDEMP"))
                if note:
                    notes.append(note)
                emp, note = _parse_numeric(record.get("EMP"))
                if note:
                    notes.append(note)
                payann, note = _parse_numeric(record.get("PAYANN"))
                if note:
                    notes.append(note)
                rcpt, note = _parse_numeric(record.get("RCPPDEMP"))
                if note:
                    notes.append(note)

                state_fips = str(record.get("state", "")).zfill(2)
                county_fips = str(record.get("county", "")).zfill(3)
                naics2 = str(record.get("NAICS2022", "")).strip()
                rows.append(
                    {
                        "year_num": year,
                        "state_cnty_fips_cd": f"{state_fips}{county_fips}",
                        "naics2_sector_cd": naics2,
                        "source_census_firmpdemp": firm,
                        "source_census_emp": emp,
                        "source_census_payann_usd": payann * 1000 if payann is not None else None,
                        "source_census_rcppdemp_usd": rcpt * 1000 if rcpt is not None else None,
                        "notes": ";".join(sorted(set(notes))) if notes else "",
                    }
                )
    return pd.DataFrame(rows)


def fetch_rdm_abs(
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
            "abs_firm_num",
            "abs_emp_num",
            "abs_payroll_usd_amt",
            "abs_rcpt_usd_amt",
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
        return df.rename(
            columns={
                "abs_firm_num": "rdm_abs_firms",
                "abs_emp_num": "rdm_abs_emp",
                "abs_payroll_usd_amt": "rdm_abs_payroll_usd_amt",
                "abs_rcpt_usd_amt": "rdm_abs_rcpt_usd_amt",
            }
        )[
            [
                "year_num",
                "state_cnty_fips_cd",
                "naics2_sector_cd",
                "rdm_abs_firms",
                "rdm_abs_emp",
                "rdm_abs_payroll_usd_amt",
                "rdm_abs_rcpt_usd_amt",
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
            abs_firm_num AS rdm_abs_firms,
            abs_emp_num AS rdm_abs_emp,
            abs_payroll_usd_amt AS rdm_abs_payroll_usd_amt,
            abs_rcpt_usd_amt AS rdm_abs_rcpt_usd_amt
        FROM `{ABS_TABLE}`
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


def fetch_rdm_abs_all(years: list[int]) -> pd.DataFrame:
    try:
        from google.cloud import bigquery
    except ImportError as exc:
        raise RuntimeError("google-cloud-bigquery is required to fetch RDM data") from exc

    client = bigquery.Client()
    query = """
        SELECT
          year_num,
          state_cnty_fips_cd,
          naics2_sector_cd,
          abs_firm_num AS rdm_abs_firms,
          abs_emp_num AS rdm_abs_emp,
          abs_payroll_usd_amt AS rdm_abs_payroll_usd_amt,
          abs_rcpt_usd_amt AS rdm_abs_rcpt_usd_amt
        FROM `rdm-datalab-portfolio.portfolio_data.econ_bnchmrk_abs_qcew`
        WHERE year_num IN UNNEST(@years)
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ArrayQueryParameter("years", "INT64", [int(y) for y in years]),
        ]
    )
    df = client.query(query, job_config=job_config).to_dataframe()
    df["state_cnty_fips_cd"] = df["state_cnty_fips_cd"].astype(str).str.zfill(5)
    df["naics2_sector_cd"] = df["naics2_sector_cd"].astype(str).str.strip()
    return df


def reconcile_abs(census_df: pd.DataFrame, rdm_df: pd.DataFrame) -> pd.DataFrame:
    merged = census_df.merge(
        rdm_df,
        how="outer",
        on=["year_num", "state_cnty_fips_cd", "naics2_sector_cd"],
        suffixes=("", "_rdm"),
    )
    merged["state_cnty_fips_cd"] = merged["state_cnty_fips_cd"].astype(str).str.zfill(5)
    merged["state_fips"] = merged["state_cnty_fips_cd"].str[:2]
    merged["county_fips"] = merged["state_cnty_fips_cd"].str[2:]
    numeric_cols = [
        "source_census_firmpdemp",
        "source_census_emp",
        "source_census_payann_usd",
        "source_census_rcppdemp_usd",
        "rdm_abs_firms",
        "rdm_abs_emp",
        "rdm_abs_payroll_usd_amt",
        "rdm_abs_rcpt_usd_amt",
    ]
    for col in numeric_cols:
        merged[col] = pd.to_numeric(merged[col], errors="coerce")

    def _delta(col_rdm: str, col_src: str) -> pd.Series:
        return merged[col_rdm] - merged[col_src]

    merged["delta_firms"] = _delta("rdm_abs_firms", "source_census_firmpdemp")
    merged["delta_emp"] = _delta("rdm_abs_emp", "source_census_emp")
    merged["delta_payroll_usd"] = _delta("rdm_abs_payroll_usd_amt", "source_census_payann_usd")
    merged["delta_receipts_usd"] = _delta("rdm_abs_rcpt_usd_amt", "source_census_rcppdemp_usd")

    merged["delta_firms_pct"] = merged.apply(
        lambda row: safe_divide(row["delta_firms"], row["source_census_firmpdemp"]), axis=1
    )
    merged["delta_emp_pct"] = merged.apply(
        lambda row: safe_divide(row["delta_emp"], row["source_census_emp"]), axis=1
    )
    merged["delta_payroll_pct"] = merged.apply(
        lambda row: safe_divide(row["delta_payroll_usd"], row["source_census_payann_usd"]), axis=1
    )
    merged["delta_receipts_pct"] = merged.apply(
        lambda row: safe_divide(row["delta_receipts_usd"], row["source_census_rcppdemp_usd"]), axis=1
    )

    def _pass_exact(delta: Any, left: Any, right: Any) -> bool:
        if pd.isna(left) or pd.isna(right):
            return False
        return delta == 0

    def _pass_tol(delta: Any, left: Any, right: Any, tol: float) -> bool:
        if pd.isna(left) or pd.isna(right):
            return False
        return abs(delta) <= tol

    merged["pass_firms"] = merged.apply(
        lambda row: _pass_exact(row["delta_firms"], row["rdm_abs_firms"], row["source_census_firmpdemp"]),
        axis=1,
    )
    merged["pass_emp"] = merged.apply(
        lambda row: _pass_exact(row["delta_emp"], row["rdm_abs_emp"], row["source_census_emp"]),
        axis=1,
    )
    merged["pass_payroll"] = merged.apply(
        lambda row: _pass_tol(row["delta_payroll_usd"], row["rdm_abs_payroll_usd_amt"], row["source_census_payann_usd"], 1000),
        axis=1,
    )
    merged["pass_receipts"] = merged.apply(
        lambda row: _pass_tol(row["delta_receipts_usd"], row["rdm_abs_rcpt_usd_amt"], row["source_census_rcppdemp_usd"], 1000),
        axis=1,
    )
    merged["pass_all"] = merged["pass_firms"] & merged["pass_emp"] & merged["pass_payroll"] & merged["pass_receipts"]

    notes = []
    for _, row in merged.iterrows():
        row_notes = row.get("notes", "")
        if pd.isna(row_notes):
            row_notes = ""
        if not isinstance(row_notes, str):
            row_notes = str(row_notes)
        flags = [row_notes] if row_notes else []
        if pd.isna(row["source_census_firmpdemp"]) and pd.isna(row["rdm_abs_firms"]):
            flags.append("missing_from_both")
        elif pd.isna(row["source_census_firmpdemp"]):
            flags.append("missing_from_census")
        elif pd.isna(row["rdm_abs_firms"]):
            flags.append("missing_from_rdm")
        notes.append(";".join([flag for flag in flags if flag]))
    merged["notes"] = notes

    keep = [
        "year_num",
        "state_cnty_fips_cd",
        "state_fips",
        "county_fips",
        "naics2_sector_cd",
        "source_census_firmpdemp",
        "source_census_emp",
        "source_census_payann_usd",
        "source_census_rcppdemp_usd",
        "rdm_abs_firms",
        "rdm_abs_emp",
        "rdm_abs_payroll_usd_amt",
        "rdm_abs_rcpt_usd_amt",
        "delta_firms",
        "delta_emp",
        "delta_payroll_usd",
        "delta_receipts_usd",
        "delta_firms_pct",
        "delta_emp_pct",
        "delta_payroll_pct",
        "delta_receipts_pct",
        "pass_firms",
        "pass_emp",
        "pass_payroll",
        "pass_receipts",
        "pass_all",
        "notes",
    ]
    return merged[keep]


def write_outputs(df: pd.DataFrame, outdir: Path, publish_bq: bool, bq_table: str) -> tuple[Path, Path]:
    outdir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    out_path = outdir / f"abs_reconciliation_{timestamp}.csv"
    latest_path = outdir / "abs_reconciliation_latest.csv"
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


def write_outputs_full(df: pd.DataFrame, outdir: Path, publish_bq: bool, bq_table: str) -> tuple[Path, Path]:
    outdir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    out_path = outdir / f"abs_reconciliation_full_{timestamp}.csv"
    latest_path = outdir / "abs_reconciliation_full_latest.csv"
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
        load_df["run_ts_utc"] = run_ts
        job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
        client.load_table_from_dataframe(load_df, bq_table, job_config=job_config).result()
    return out_path, latest_path


def parse_args(argv: Optional[list[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="ABS Census API reconciliation QA.")
    parser.add_argument("--years", nargs="+", type=int, default=DEFAULT_YEARS)
    parser.add_argument("--counties", nargs="+", default=DEFAULT_COUNTIES)
    parser.add_argument("--naics", nargs="+", default=DEFAULT_NAICS)
    parser.add_argument("--outdir", default=DEFAULT_OUTDIR)
    parser.add_argument("--publish_bq", default="false")
    parser.add_argument("--bq_table", default="rdm-datalab-portfolio.portfolio_data.qa_abs_reconciliation")
    parser.add_argument("--rdm_csv", default=None)
    return parser.parse_args(argv)


def run(config: AbsConfig) -> pd.DataFrame:
    census_df = fetch_census_data(config.years, config.counties, config.naics)
    rdm_df = fetch_rdm_abs(config.years, config.counties, config.naics, config.rdm_csv)
    return reconcile_abs(census_df, rdm_df)


def run_full_surface(years: list[int]) -> pd.DataFrame:
    census_df = fetch_census_data_states(years)
    rdm_df = fetch_rdm_abs_all(years)
    return reconcile_abs(census_df, rdm_df)


def main(argv: Optional[list[str]] = None) -> None:
    args = parse_args(argv)
    config = AbsConfig(
        years=args.years,
        counties=[str(c).zfill(5) for c in args.counties],
        naics=[str(n).zfill(2) for n in args.naics],
        outdir=Path(args.outdir),
        publish_bq=parse_bool(args.publish_bq),
        bq_table=args.bq_table,
        rdm_csv=Path(args.rdm_csv) if args.rdm_csv else None,
    )
    df = run(config)
    out_path, latest_path = write_outputs(df, config.outdir, config.publish_bq, config.bq_table)
    total = len(df)
    passed = int(df["pass_all"].sum()) if total else 0
    failures = df[df["pass_all"] == False]
    print(f"[ABS] Wrote {out_path} and {latest_path}")
    print(f"[ABS] pass_all: {passed}/{total}")
    if not failures.empty:
        print("[ABS] Failures:")
        for _, row in failures.iterrows():
            print(
                f"  - {row['year_num']} {row['state_cnty_fips_cd']} {row['naics2_sector_cd']}: "
                f"firms={row['pass_firms']} emp={row['pass_emp']} "
                f"payroll={row['pass_payroll']} receipts={row['pass_receipts']}"
            )


if __name__ == "__main__":
    main()
