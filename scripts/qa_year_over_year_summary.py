#!/usr/bin/env python3
"""
Year-over-Year QA Summary (ABS + QCEW)
-------------------------------------
Purpose
  Summarize 2022 vs 2023 year-over-year behavior for ABS + QCEW using
  BigQuery as system of record. Writes compact evidence artifacts for review.

Outputs (written under outputs/qa/):
  - qa_rollup_totals_2022_2023.csv
  - qa_rollup_totals_2022_2023.md
  - qa_naics2_deltas_2022_2023.csv
  - qa_ratio_deltas_2022_2023.csv
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

import pandas as pd


DEFAULT_PROJECT = "rdm-datalab-portfolio"
DEFAULT_DATASET = "portfolio_data"
DEFAULT_TABLE = "econ_bnchmrk_abs_qcew"
DEFAULT_YEARS = [2022, 2023]

OUTPUT_DIR = Path("outputs/qa")


def safe_div(n: Any, d: Any) -> Optional[float]:
    try:
        if d is None:
            return None
        if pd.isna(d) or d <= 0:
            return None
        if pd.isna(n):
            return None
        return float(n) / float(d)
    except Exception:
        return None


def _get_bq_client():
    try:
        from google.cloud import bigquery  # type: ignore
    except Exception:
        return None
    return bigquery.Client()


def _query_df(project: str, query: str, params: Optional[dict] = None) -> pd.DataFrame:
    client = _get_bq_client()
    if client is not None:
        from google.cloud import bigquery  # type: ignore

        job_config = bigquery.QueryJobConfig()
        if params:
            query_params = []
            for name, value in params.items():
                if isinstance(value, list):
                    query_params.append(bigquery.ArrayQueryParameter(name, "INT64", value))
                else:
                    query_params.append(bigquery.ScalarQueryParameter(name, "STRING", value))
            job_config.query_parameters = query_params
        return client.query(query, job_config=job_config).to_dataframe(
            create_bqstorage_client=False
        )

    try:
        import pandas_gbq  # type: ignore
    except Exception as exc:
        raise RuntimeError(
            "google-cloud-bigquery or pandas-gbq is required to query BigQuery."
        ) from exc

    if params:
        for name, value in params.items():
            if isinstance(value, list):
                literal = "(" + ",".join(str(v) for v in value) + ")"
                query = query.replace(f"UNNEST(@{name})", literal)
                query = query.replace("@" + name, literal)
            else:
                query = query.replace("@" + name, f"'{value}'")
    return pandas_gbq.read_gbq(query, project_id=project, dialect="standard")


def _fetch_columns(project: str, dataset: str, table: str) -> List[str]:
    query = f"""
        SELECT column_name
        FROM `{project}.{dataset}.INFORMATION_SCHEMA.COLUMNS`
        WHERE table_name = @table
    """
    df = _query_df(project, query, params={"table": table})
    return [str(c) for c in df["column_name"].tolist()]


def _pick_column(candidates: Iterable[str], available: set[str]) -> Optional[str]:
    for cand in candidates:
        if cand in available:
            return cand
    return None


def _metric_columns(available: set[str]) -> Dict[str, Optional[str]]:
    return {
        "abs_firms": _pick_column(["abs_firm_num", "abs_firms", "abs_firms_num"], available),
        "abs_emp": _pick_column(["abs_emp_num", "abs_emp", "abs_employment_num"], available),
        "abs_payroll": _pick_column(
            ["abs_payroll_usd_amt", "abs_payroll", "abs_payroll_usd"], available
        ),
        "abs_receipts": _pick_column(
            ["abs_rcpt_usd_amt", "abs_rcpt", "abs_receipts_usd_amt", "abs_receipts_usd"],
            available,
        ),
        "qcew_emp": _pick_column(
            ["qcew_ann_avg_emp_lvl_num", "qcew_emp", "qcew_employment_num"], available
        ),
        "qcew_wages": _pick_column(
            ["qcew_ttl_ann_wage_usd_amt", "qcew_wages_usd_amt", "qcew_wages"],
            available,
        ),
        "qcew_avg_weekly_wage": _pick_column(
            [
                "qcew_avg_wkly_wage_usd_amt",
                "qcew_avg_weekly_wage_usd_amt",
                "qcew_avg_wkly_wage",
                "qcew_avg_weekly_wage",
            ],
            available,
        ),
    }


def _build_rollup_query(
    project: str, dataset: str, table: str, years: List[int], cols: Dict[str, Optional[str]]
) -> str:
    select_exprs = [
        "year_num",
        "naics2_sector_cd",
    ]
    for alias, col in cols.items():
        if col is None:
            continue
        if alias == "qcew_avg_weekly_wage" and cols.get("qcew_emp"):
            select_exprs.append(
                f"SUM({col} * {cols['qcew_emp']}) AS qcew_avg_weekly_wage_weighted"
            )
            select_exprs.append(f"SUM({cols['qcew_emp']}) AS qcew_emp_for_avg")
        else:
            select_exprs.append(f"SUM({col}) AS {alias}")
    select_sql = ",\n            ".join(select_exprs)
    query = f"""
        SELECT
            {select_sql}
        FROM `{project}.{dataset}.{table}`
        WHERE year_num IN UNNEST(@years)
        GROUP BY year_num, naics2_sector_cd
    """
    return query


def _coverage_query(project: str, dataset: str, table: str, years: List[int]) -> str:
    return f"""
        SELECT
          year_num,
          COUNT(*) AS row_count,
          COUNT(DISTINCT naics2_sector_cd) AS naics2_count
        FROM `{project}.{dataset}.{table}`
        WHERE year_num IN UNNEST(@years)
        GROUP BY year_num
        ORDER BY year_num
    """


def _compute_avg_weekly_wage(df: pd.DataFrame, cols: Dict[str, Optional[str]]) -> pd.Series:
    if "qcew_avg_weekly_wage_weighted" in df.columns and "qcew_emp_for_avg" in df.columns:
        return df.apply(
            lambda r: safe_div(r["qcew_avg_weekly_wage_weighted"], r["qcew_emp_for_avg"]),
            axis=1,
        )
    if "qcew_wages" in df.columns and "qcew_emp" in df.columns:
        return df.apply(
            lambda r: safe_div(r["qcew_wages"], r["qcew_emp"] * 52.0),
            axis=1,
        )
    return pd.Series([None] * len(df))


def _validate_rollups(national: pd.DataFrame, naics: pd.DataFrame, years: List[int]) -> None:
    if set(national["year_num"].tolist()) != set(years) or len(national) != len(years):
        raise ValueError("National rollup does not contain exactly the requested years.")
    if national.groupby("year_num").size().max() != 1:
        raise ValueError("National rollup does not have exactly one row per year.")
    if naics.groupby(["naics2_sector_cd", "year_num"]).size().max() != 1:
        raise ValueError("NAICS-2 rollup does not have exactly one row per (naics2, year).")

    core_cols = [
        "abs_firms",
        "abs_emp",
        "abs_payroll",
        "abs_receipts",
        "qcew_emp",
        "qcew_wages",
        "qcew_avg_weekly_wage",
    ]
    for col in core_cols:
        if col in national.columns:
            if (national[col].dropna() < 0).any():
                raise ValueError(f"Negative values found in national rollup column: {col}")
        if col in naics.columns:
            if (naics[col].dropna() < 0).any():
                raise ValueError(f"Negative values found in NAICS rollup column: {col}")


def _pct_delta(v2023: Any, v2022: Any) -> Optional[float]:
    if v2022 is None or v2023 is None:
        return None
    if pd.isna(v2022) or pd.isna(v2023):
        return None
    return safe_div(v2023 - v2022, v2022)


def _build_totals_md(totals: pd.DataFrame, years: List[int]) -> str:
    totals = totals.sort_values("year_num")
    y2022 = totals[totals["year_num"] == years[0]].iloc[0]
    y2023 = totals[totals["year_num"] == years[1]].iloc[0]

    metrics = [
        "abs_firms",
        "abs_emp",
        "abs_payroll",
        "abs_receipts",
        "qcew_emp",
        "qcew_wages",
        "qcew_avg_weekly_wage",
    ]
    rows = []
    for m in metrics:
        if m not in totals.columns:
            continue
        v2022 = y2022.get(m)
        v2023 = y2023.get(m)
        rows.append(
            {
                "metric": m,
                "2022": v2022,
                "2023": v2023,
                "pct_change": _pct_delta(v2023, v2022),
            }
        )

    md = [
        "# QA Year-over-Year Summary (2022 → 2023)",
        "",
        "National totals across ABS + QCEW metrics.",
        "",
        "| Metric | 2022 | 2023 | % Change |",
        "| --- | ---: | ---: | ---: |",
    ]
    for row in rows:
        pct = row["pct_change"]
        pct_fmt = f"{pct:.4%}" if pct is not None else "NA"
        md.append(f"| {row['metric']} | {row['2022']} | {row['2023']} | {pct_fmt} |")
    md.append("")
    return "\n".join(md)


def _build_naics2_deltas(naics: pd.DataFrame, years: List[int]) -> pd.DataFrame:
    metrics = [
        "abs_firms",
        "abs_emp",
        "abs_payroll",
        "abs_receipts",
        "qcew_emp",
        "qcew_wages",
        "qcew_avg_weekly_wage",
    ]
    naics = naics[["year_num", "naics2_sector_cd"] + [m for m in metrics if m in naics.columns]].copy()
    pivot = {}
    for year in years:
        pivot[year] = naics[naics["year_num"] == year].set_index("naics2_sector_cd")

    rows = []
    for metric in metrics:
        if metric not in naics.columns:
            continue
        left = pivot[years[0]][metric]
        right = pivot[years[1]][metric]
        merged = pd.DataFrame(
            {
                "naics2": left.index,
                "metric": metric,
                "2022_value": left.values,
                "2023_value": right.reindex(left.index).values,
            }
        )
        merged["abs_delta"] = merged["2023_value"] - merged["2022_value"]
        merged["pct_delta"] = merged.apply(
            lambda r: safe_div(r["abs_delta"], r["2022_value"]), axis=1
        )

        top_pos = merged.sort_values("abs_delta", ascending=False).head(10)
        top_neg = merged.sort_values("abs_delta", ascending=True).head(10)
        rows.append(pd.concat([top_pos, top_neg], ignore_index=True))
    if not rows:
        return pd.DataFrame(columns=["naics2", "metric", "2022_value", "2023_value", "abs_delta", "pct_delta"])
    return pd.concat(rows, ignore_index=True)


def _build_ratio_deltas(
    national: pd.DataFrame, naics: pd.DataFrame, years: List[int]
) -> pd.DataFrame:
    ratio_defs = {
        "abs_receipts_per_firm": ("abs_receipts", "abs_firms"),
        "abs_receipts_per_emp": ("abs_receipts", "abs_emp"),
        "abs_payroll_per_emp": ("abs_payroll", "abs_emp"),
        "abs_payroll_to_receipts": ("abs_payroll", "abs_receipts"),
        "qcew_wage_per_emp": ("qcew_wages", "qcew_emp"),
    }

    def add_ratios(df: pd.DataFrame) -> pd.DataFrame:
        out = df.copy()
        for ratio, (num, den) in ratio_defs.items():
            if num in out.columns and den in out.columns:
                out[ratio] = out.apply(lambda r: safe_div(r[num], r[den]), axis=1)
        return out

    national_r = add_ratios(national)
    naics_r = add_ratios(naics)

    rows = []
    def _safe_delta(v2023: Any, v2022: Any) -> Optional[float]:
        if v2022 is None or v2023 is None:
            return None
        if pd.isna(v2022) or pd.isna(v2023):
            return None
        return v2023 - v2022

    for ratio in ratio_defs.keys():
        if ratio not in national_r.columns:
            continue
        nat_2022 = national_r[national_r["year_num"] == years[0]][ratio].iloc[0]
        nat_2023 = national_r[national_r["year_num"] == years[1]][ratio].iloc[0]
        nat_delta = _safe_delta(nat_2023, nat_2022)
        rows.append(
            {
                "grain": "national",
                "naics2": "",
                "metric": ratio,
                "2022_value": nat_2022,
                "2023_value": nat_2023,
                "abs_delta": nat_delta,
                "pct_delta": safe_div(nat_delta, nat_2022),
            }
        )

        if "naics2_sector_cd" in naics_r.columns:
            left = naics_r[naics_r["year_num"] == years[0]].set_index("naics2_sector_cd")[ratio]
            right = naics_r[naics_r["year_num"] == years[1]].set_index("naics2_sector_cd")[ratio]
            for naics2 in left.index:
                v2022 = left.loc[naics2]
                v2023 = right.reindex(left.index).loc[naics2]
                delta = _safe_delta(v2023, v2022)
                rows.append(
                    {
                        "grain": "naics2",
                        "naics2": naics2,
                        "metric": ratio,
                        "2022_value": v2022,
                        "2023_value": v2023,
                        "abs_delta": delta,
                        "pct_delta": safe_div(delta, v2022),
                    }
                )
    return pd.DataFrame(rows)


def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="QA year-over-year summary for ABS + QCEW.")
    parser.add_argument("--years", nargs="+", type=int, default=DEFAULT_YEARS)
    parser.add_argument("--project", default=DEFAULT_PROJECT)
    parser.add_argument("--dataset", default=DEFAULT_DATASET)
    parser.add_argument("--table", default=DEFAULT_TABLE)
    return parser.parse_args(argv)


def main(argv: Optional[List[str]] = None) -> int:
    args = parse_args(argv)
    years = sorted(args.years)
    if len(years) != 2:
        raise ValueError("Exactly two years are required for year-over-year comparison.")

    columns = _fetch_columns(args.project, args.dataset, args.table)
    available = set(columns)
    required = {"year_num", "naics2_sector_cd"}
    missing_required = required - available
    if missing_required:
        raise RuntimeError(f"Missing required columns in BigQuery table: {sorted(missing_required)}")

    cols = _metric_columns(available)
    if not any(cols.values()):
        raise RuntimeError("No metric columns found in BigQuery table.")

    coverage_df = _query_df(
        args.project,
        _coverage_query(args.project, args.dataset, args.table, years),
        params={"years": years},
    )
    for _, row in coverage_df.iterrows():
        print(
            f"[coverage] year={row['year_num']} rows={row['row_count']} naics2={row['naics2_count']}"
        )

    rollup_df = _query_df(
        args.project,
        _build_rollup_query(args.project, args.dataset, args.table, years, cols),
        params={"years": years},
    )

    for col in rollup_df.columns:
        if col in {"year_num", "naics2_sector_cd"}:
            continue
        rollup_df[col] = pd.to_numeric(rollup_df[col], errors="coerce")

    rollup_df["qcew_avg_weekly_wage"] = _compute_avg_weekly_wage(rollup_df, cols)

    naics = rollup_df.rename(columns={"naics2_sector_cd": "naics2_sector_cd"}).copy()
    drop_cols = ["naics2_sector_cd"]
    if "qcew_avg_weekly_wage" in naics.columns:
        drop_cols.append("qcew_avg_weekly_wage")
    national = (
        naics.drop(columns=drop_cols)
        .groupby("year_num", as_index=False)
        .sum(numeric_only=True)
    )
    national["qcew_avg_weekly_wage"] = _compute_avg_weekly_wage(national, cols)

    _validate_rollups(national, naics, years)

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    totals_path = OUTPUT_DIR / f"qa_rollup_totals_{years[0]}_{years[1]}.csv"
    totals_md_path = OUTPUT_DIR / f"qa_rollup_totals_{years[0]}_{years[1]}.md"
    naics_delta_path = OUTPUT_DIR / f"qa_naics2_deltas_{years[0]}_{years[1]}.csv"
    ratio_delta_path = OUTPUT_DIR / f"qa_ratio_deltas_{years[0]}_{years[1]}.csv"

    totals_cols = [
        "year_num",
        "abs_firms",
        "abs_emp",
        "abs_payroll",
        "abs_receipts",
        "qcew_emp",
        "qcew_wages",
        "qcew_avg_weekly_wage",
    ]
    totals = national[[c for c in totals_cols if c in national.columns]].copy()
    totals = totals.rename(columns={"year_num": "year"})
    totals.to_csv(totals_path, index=False)

    md = _build_totals_md(totals.rename(columns={"year": "year_num"}), years)
    totals_md_path.write_text(md, encoding="utf-8")

    naics_deltas = _build_naics2_deltas(naics, years)
    naics_deltas.to_csv(naics_delta_path, index=False)

    ratio_deltas = _build_ratio_deltas(national, naics, years)
    ratio_deltas.to_csv(ratio_delta_path, index=False)

    print("National percent deltas (2022 -> 2023):")
    for metric in [m for m in totals.columns if m != "year"]:
        v2022 = totals[totals["year"] == years[0]].iloc[0].get(metric)
        v2023 = totals[totals["year"] == years[1]].iloc[0].get(metric)
        pct = _pct_delta(v2023, v2022)
        pct_fmt = f"{pct:.4%}" if pct is not None else "NA"
        print(f"  {metric}: {pct_fmt}")

    print("Outputs:")
    print(f"  {totals_path}")
    print(f"  {totals_md_path}")
    print(f"  {naics_delta_path}")
    print(f"  {ratio_delta_path}")

    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        raise
