#!/usr/bin/env python3
"""
Freeze national ABS/QCEW totals by year and write a markdown snapshot.

Usage:
  python -m qa.national_totals_snapshot --years 2022 2023 \
      --outpath artifacts/qa/national_totals_snapshot.md
"""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Iterable, Optional

import pandas as pd


# Default parameters for a standard release snapshot.
DEFAULT_YEARS = [2022, 2023]
DEFAULT_OUTPATH = "artifacts/qa/national_totals_snapshot.md"
TABLE_NAME = "rdm-datalab-portfolio.portfolio_data.econ_bnchmrk_abs_qcew"


def parse_args(argv: Optional[list[str]] = None) -> argparse.Namespace:
    # CLI wrapper lets us pin the run to a specific release window and
    # optionally change the output location.
    parser = argparse.ArgumentParser(description="Write national totals snapshot.")
    parser.add_argument("--years", nargs="+", type=int, default=DEFAULT_YEARS)
    parser.add_argument("--outpath", default=DEFAULT_OUTPATH)
    return parser.parse_args(argv)


def log(msg: str) -> None:
    # Keep logs minimal but explicit; this is a debug-friendly utility.
    print(f"[NATIONAL SNAPSHOT] {msg}")


def _to_decimal(value: object) -> Optional[Decimal]:
    # Normalize numeric inputs (including BigQuery decimals) to Decimal so
    # we can do safe math without float precision surprises.
    if value is None:
        return None
    if isinstance(value, Decimal):
        return value
    try:
        return Decimal(str(value))
    except Exception:
        return None


def fmt_int(value: Optional[Decimal]) -> str:
    # Format integers with commas, consistent across runs.
    if value is None:
        return "—"
    return f"{int(value):,}"


def fmt_usd(value: Optional[Decimal]) -> str:
    # Format currency with a dollar sign and commas.
    if value is None:
        return "—"
    return f"${int(value):,}"


def fmt_pct(value: Optional[float]) -> str:
    # Percent output with 2 decimals for deterministic markdown.
    if value is None:
        return "—"
    return f"{value * 100:.2f}%"


def safe_divide(numerator: Optional[Decimal], denominator: Optional[Decimal]) -> Optional[float]:
    # Avoid division-by-zero and missing values for YoY percentage math.
    if numerator is None or denominator is None:
        return None
    if denominator == 0:
        return None
    return float(numerator / denominator)


def fetch_totals(years: Iterable[int]) -> pd.DataFrame:
    # Single BigQuery query, grouped by year, for deterministic rollups.
    try:
        from google.cloud import bigquery
    except ImportError as exc:
        raise RuntimeError("google-cloud-bigquery is required to fetch totals") from exc

    years_list = sorted({int(y) for y in years})
    log(f"Querying BigQuery for years={years_list}...")
    query = f"""
        SELECT
          year_num,
          COUNT(*) AS row_cnt,
          COUNT(DISTINCT state_cnty_fips_cd) AS county_cnt,
          COUNT(DISTINCT naics2_sector_cd) AS naics2_cnt,
          SUM(abs_firm_num) AS abs_firms_natl,
          SUM(abs_emp_num) AS abs_emp_natl,
          SUM(abs_payroll_usd_amt) AS abs_payroll_usd_natl,
          SUM(abs_rcpt_usd_amt) AS abs_receipts_usd_natl,
          SUM(qcew_ann_avg_emp_lvl_num) AS qcew_emp_natl,
          SUM(qcew_ttl_ann_wage_usd_amt) AS qcew_wages_usd_natl
        FROM `{TABLE_NAME}`
        WHERE year_num IN UNNEST(@years)
        GROUP BY year_num
        ORDER BY year_num
    """
    client = bigquery.Client()
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ArrayQueryParameter("years", "INT64", years_list),
        ]
    )
    df = client.query(query, job_config=job_config).to_dataframe()
    if df.empty:
        raise RuntimeError(f"No rows returned for years: {years_list}")
    log(f"Received {len(df)} year rows from BigQuery.")
    return df


def build_yoy_table(df: pd.DataFrame) -> list[dict[str, object]]:
    # Compute YoY changes for consecutive years only (after sorting).
    rows: list[dict[str, object]] = []
    df = df.sort_values("year_num")
    records = df.to_dict(orient="records")
    for idx in range(1, len(records)):
        cur = records[idx]
        prev = records[idx - 1]
        rows.append(
            {
                "year_num": cur["year_num"],
                "yoy_abs_receipts_pct": safe_divide(
                    _to_decimal(cur["abs_receipts_usd_natl"]) - _to_decimal(prev["abs_receipts_usd_natl"]),
                    _to_decimal(prev["abs_receipts_usd_natl"]),
                ),
                "yoy_abs_payroll_pct": safe_divide(
                    _to_decimal(cur["abs_payroll_usd_natl"]) - _to_decimal(prev["abs_payroll_usd_natl"]),
                    _to_decimal(prev["abs_payroll_usd_natl"]),
                ),
                "yoy_abs_emp_pct": safe_divide(
                    _to_decimal(cur["abs_emp_natl"]) - _to_decimal(prev["abs_emp_natl"]),
                    _to_decimal(prev["abs_emp_natl"]),
                ),
                "yoy_abs_firms_pct": safe_divide(
                    _to_decimal(cur["abs_firms_natl"]) - _to_decimal(prev["abs_firms_natl"]),
                    _to_decimal(prev["abs_firms_natl"]),
                ),
                "yoy_qcew_emp_pct": safe_divide(
                    _to_decimal(cur["qcew_emp_natl"]) - _to_decimal(prev["qcew_emp_natl"]),
                    _to_decimal(prev["qcew_emp_natl"]),
                ),
                "yoy_qcew_wages_pct": safe_divide(
                    _to_decimal(cur["qcew_wages_usd_natl"]) - _to_decimal(prev["qcew_wages_usd_natl"]),
                    _to_decimal(prev["qcew_wages_usd_natl"]),
                ),
            }
        )
    return rows


def write_markdown(outpath: Path, df: pd.DataFrame) -> None:
    # Render snapshot markdown with deterministic formatting and ordering.
    timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    totals = df.sort_values("year_num").to_dict(orient="records")
    yoy_rows = build_yoy_table(df)

    log(f"Rendering markdown to {outpath}...")
    lines: list[str] = []
    lines.append("# National totals snapshot")
    lines.append("")
    lines.append(f"- Run timestamp (UTC): {timestamp}")
    lines.append(f"- Source table: `{TABLE_NAME}`")
    lines.append("")
    lines.append("## Totals by year")
    lines.append("")
    lines.append(
        "| year | abs_firms_natl | abs_emp_natl | abs_payroll_usd_natl | abs_receipts_usd_natl | "
        "qcew_emp_natl | qcew_wages_usd_natl |"
    )
    lines.append("| --- | --- | --- | --- | --- | --- | --- |")
    for row in totals:
        lines.append(
            "| {year} | {firms} | {emp} | {payroll} | {receipts} | {qcew_emp} | {qcew_wages} |".format(
                year=row["year_num"],
                firms=fmt_int(_to_decimal(row["abs_firms_natl"])),
                emp=fmt_int(_to_decimal(row["abs_emp_natl"])),
                payroll=fmt_usd(_to_decimal(row["abs_payroll_usd_natl"])),
                receipts=fmt_usd(_to_decimal(row["abs_receipts_usd_natl"])),
                qcew_emp=fmt_int(_to_decimal(row["qcew_emp_natl"])),
                qcew_wages=fmt_usd(_to_decimal(row["qcew_wages_usd_natl"])),
            )
        )
    lines.append("")

    if yoy_rows:
        lines.append("## YoY percent changes")
        lines.append("")
        lines.append(
            "| year | yoy_abs_receipts_pct | yoy_abs_payroll_pct | yoy_abs_emp_pct | "
            "yoy_abs_firms_pct | yoy_qcew_emp_pct | yoy_qcew_wages_pct |"
        )
        lines.append("| --- | --- | --- | --- | --- | --- | --- |")
        for row in yoy_rows:
            lines.append(
                "| {year} | {receipts} | {payroll} | {emp} | {firms} | {qcew_emp} | {qcew_wages} |".format(
                    year=row["year_num"],
                    receipts=fmt_pct(row["yoy_abs_receipts_pct"]),
                    payroll=fmt_pct(row["yoy_abs_payroll_pct"]),
                    emp=fmt_pct(row["yoy_abs_emp_pct"]),
                    firms=fmt_pct(row["yoy_abs_firms_pct"]),
                    qcew_emp=fmt_pct(row["yoy_qcew_emp_pct"]),
                    qcew_wages=fmt_pct(row["yoy_qcew_wages_pct"]),
                )
            )
        lines.append("")

    lines.append("## Coverage")
    lines.append("")
    lines.append("| year | row_cnt | county_cnt | naics2_cnt |")
    lines.append("| --- | --- | --- | --- |")
    for row in totals:
        lines.append(
            "| {year} | {row_cnt} | {county_cnt} | {naics2_cnt} |".format(
                year=row["year_num"],
                row_cnt=fmt_int(_to_decimal(row["row_cnt"])),
                county_cnt=fmt_int(_to_decimal(row["county_cnt"])),
                naics2_cnt=fmt_int(_to_decimal(row["naics2_cnt"])),
            )
        )
    lines.append("")

    outpath.parent.mkdir(parents=True, exist_ok=True)
    outpath.write_text("\n".join(lines))
    log("Markdown snapshot written.")


def main(argv: Optional[list[str]] = None) -> None:
    # Entry point: parse args, query totals, compute YoY, and write snapshot.
    args = parse_args(argv)
    outpath = Path(args.outpath)
    log(f"Starting snapshot (years={sorted({int(y) for y in args.years})}).")
    df = fetch_totals(args.years)
    write_markdown(outpath, df)
    years_sorted = sorted({int(y) for y in args.years})
    log(f"years={years_sorted}")
    log(f"wrote={outpath}")


if __name__ == "__main__":
    main()
