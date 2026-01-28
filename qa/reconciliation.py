#!/usr/bin/env python3
"""
Combined ABS + QCEW Reconciliation Runner
-----------------------------------------
Purpose
  Run ABS and/or QCEW reconciliations and emit combined outputs + summary.

How to run
  python -m qa.reconciliation --systems abs qcew --years 2022 2023 \
      --counties 06075 06085 --naics 42 62 --outdir artifacts/qa --publish_bq false
"""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import pandas as pd

from qa.abs_reconciliation import (
    AbsConfig,
    run as run_abs,
    run_full_surface as run_abs_full_surface,
    write_outputs_full as write_abs_outputs_full,
)
from qa.qcew_reconciliation import QcewConfig, run as run_qcew
from qa.utils import parse_bool


DEFAULT_OUTDIR = "artifacts/qa"
DEFAULT_ABS_FULL_BQ_TABLE = "rdm-datalab-portfolio.portfolio_data.qa_abs_reconciliation_full"


def parse_args(argv: Optional[list[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run ABS/QCEW reconciliation QA.")
    parser.add_argument("--mode", default="standard")
    parser.add_argument("--systems", nargs="+", default=["abs", "qcew"])
    parser.add_argument("--years", nargs="+", type=int, default=[2022, 2023])
    parser.add_argument("--counties", nargs="+", default=["06075", "06085"])
    parser.add_argument("--naics", nargs="+", default=["42", "62"])
    parser.add_argument("--outdir", default=DEFAULT_OUTDIR)
    parser.add_argument("--publish_bq", default="false")
    parser.add_argument("--bq_table", default=DEFAULT_ABS_FULL_BQ_TABLE)
    parser.add_argument("--rdm_csv", default=None)
    return parser.parse_args(argv)


def log(msg: str) -> None:
    """Lightweight logger for terminal visibility during runs."""
    print(f"[RECON] {msg}")


def write_summary(outdir: Path, abs_df: pd.DataFrame | None, qcew_df: pd.DataFrame | None) -> Path:
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    summary_path = outdir / f"reconciliation_summary_{timestamp}.md"
    lines: list[str] = ["# Reconciliation Summary", ""]
    if abs_df is not None:
        total = len(abs_df)
        passed = int(abs_df["pass_all"].sum()) if total else 0
        failures = abs_df[abs_df["pass_all"] == False]
        lines.append(f"ABS pass_all: {passed}/{total}")
        if not failures.empty:
            lines.append("ABS failures:")
            for _, row in failures.iterrows():
                lines.append(
                    f"- {row['year_num']} {row['state_cnty_fips_cd']} {row['naics2_sector_cd']}: "
                    f"firms={row['pass_firms']} emp={row['pass_emp']} "
                    f"payroll={row['pass_payroll']} receipts={row['pass_receipts']}"
                )
        lines.append("")
    if qcew_df is not None:
        total = len(qcew_df)
        passed = int((qcew_df["pass_all"] == True).sum()) if total else 0
        failures = qcew_df[qcew_df["pass_all"] == False]
        lines.append(f"QCEW pass_all: {passed}/{total}")
        if not failures.empty:
            lines.append("QCEW failures:")
            for _, row in failures.iterrows():
                lines.append(
                    f"- {row['year_num']} {row['state_cnty_fips_cd']} {row['naics2_sector_cd']}: "
                    f"emp={row['pass_emp']} wages={row['pass_wages']} avg_weekly_wage={row['pass_avg_weekly_wage']}"
                )
        lines.append("")
    summary_path.write_text("\n".join(lines))
    return summary_path


def main(argv: Optional[list[str]] = None) -> None:
    # ---------------------------
    # Parse CLI args and prepare
    # ---------------------------
    args = parse_args(argv)
    mode = args.mode.lower()
    systems = [s.lower() for s in args.systems]
    outdir = Path(args.outdir)
    outdir.mkdir(parents=True, exist_ok=True)
    publish_bq = parse_bool(args.publish_bq)
    log(f"Systems: {systems}")
    log(f"Mode: {mode}")
    log(f"Years: {args.years}")
    log(f"Counties: {args.counties}")
    log(f"NAICS: {args.naics}")
    log(f"Output dir: {outdir}")
    log(f"Publish to BigQuery: {publish_bq}")
    log(f"BigQuery table: {args.bq_table}")
    log(f"RDM CSV override: {args.rdm_csv or 'None'}")
    abs_df = None
    qcew_df = None

    if mode == "abs_full_surface":
        log("Starting ABS full-surface reconciliation...")
        try:
            abs_df = run_abs_full_surface(args.years)
        except Exception as exc:
            log(f"ABS full-surface reconciliation failed: {exc!r}")
            raise
        abs_df = abs_df.copy()
        abs_df["source_system"] = "abs"
        out_path, latest_path = write_abs_outputs_full(abs_df, outdir, publish_bq, args.bq_table)
        total = len(abs_df)
        passed = int(abs_df["pass_all"].sum()) if total else 0
        failures = abs_df[abs_df["pass_all"] == False]
        log(f"Wrote {out_path} and {latest_path}")
        log(f"ABS pass_all: {passed}/{total}")
        if not failures.empty:
            log("ABS failures:")
            for _, row in failures.iterrows():
                log(
                    f"  - {row['year_num']} {row['state_cnty_fips_cd']} {row['naics2_sector_cd']}: "
                    f"firms={row['pass_firms']} emp={row['pass_emp']} "
                    f"payroll={row['pass_payroll']} receipts={row['pass_receipts']}"
                )
        return

    if "abs" in systems:
        # ---------------------------
        # ABS reconciliation workflow
        # ---------------------------
        log("Starting ABS reconciliation...")
        abs_config = AbsConfig(
            years=args.years,
            counties=[str(c).zfill(5) for c in args.counties],
            naics=[str(n).zfill(2) for n in args.naics],
            outdir=outdir,
            publish_bq=publish_bq,
            bq_table="rdm-datalab-portfolio.portfolio_data.qa_abs_reconciliation",
            rdm_csv=Path(args.rdm_csv) if args.rdm_csv else None,
        )
        try:
            abs_df = run_abs(abs_config)
        except Exception as exc:
            log(f"ABS reconciliation failed: {exc!r}")
            raise
        abs_df = abs_df.copy()
        abs_df["source_system"] = "abs"
        log(f"ABS reconciliation complete (rows: {len(abs_df)}).")

    if "qcew" in systems:
        # ---------------------------
        # QCEW reconciliation workflow
        # ---------------------------
        log("Starting QCEW reconciliation...")
        qcew_config = QcewConfig(
            years=args.years,
            counties=[str(c).zfill(5) for c in args.counties],
            naics=[str(n).zfill(2) for n in args.naics],
            outdir=outdir,
            publish_bq=publish_bq,
            bq_table="rdm-datalab-portfolio.portfolio_data.qa_qcew_reconciliation",
            raw_template="data_raw/qcew/{year}.annual.singlefile.csv",
            cache_dir=Path("data_raw/qcew/source_qa"),
            ownership_code="5",
            agg_level="74",
            allow_wage_tolerance=True,
            rdm_csv=Path(args.rdm_csv) if args.rdm_csv else None,
        )
        try:
            qcew_df = run_qcew(qcew_config)
        except Exception as exc:
            log(f"QCEW reconciliation failed: {exc!r}")
            raise
        qcew_df = qcew_df.copy()
        qcew_df["source_system"] = "qcew"
        log(f"QCEW reconciliation complete (rows: {len(qcew_df)}).")

    # ---------------------------
    # Combine outputs and persist
    # ---------------------------
    combined = None
    if abs_df is not None and qcew_df is not None:
        combined = pd.concat([abs_df, qcew_df], ignore_index=True, sort=False)
    elif abs_df is not None:
        combined = abs_df
    elif qcew_df is not None:
        combined = qcew_df

    if combined is not None:
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        combined_path = outdir / f"reconciliation_all_{timestamp}.csv"
        combined.to_csv(combined_path, index=False)
        log(f"Wrote combined CSV: {combined_path}")

    summary_path = write_summary(outdir, abs_df, qcew_df)
    log(f"Wrote summary: {summary_path}")


if __name__ == "__main__":
    main()
