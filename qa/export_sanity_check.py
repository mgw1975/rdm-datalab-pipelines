#!/usr/bin/env python3
import argparse
import json
import os
import re
import sys
from datetime import datetime

import pandas as pd

# Explicit column mapping for known exports.
# Update these lists if column names drift in the source exports.
COLUMN_MAP = {
    "fact": {
        "year_num": ["year_num", "year"],
        "state_cnty_fips_cd": ["state_cnty_fips_cd", "county_fips", "fips"],
        "naics2_sector_cd": ["naics2_sector_cd", "naics2", "naics2_cd"],
        "abs_firms": ["abs_firms", "abs_num_firms", "abs_firm_num"],
        "abs_emp": ["abs_emp", "abs_employment", "abs_emp_num"],
        "abs_payroll_usd_amt": ["abs_payroll_usd_amt", "abs_payroll"],
        "abs_rcpt_usd_amt": ["abs_rcpt_usd_amt", "abs_receipts"],
        "qcew_emp": ["qcew_emp", "qcew_employment", "qcew_ann_avg_emp_lvl_num"],
        "qcew_wages_usd": ["qcew_wages_usd", "qcew_wages", "qcew_ttl_ann_wage_usd_amt"],
        "qcew_avg_weekly_wage_usd": [
            "qcew_avg_weekly_wage_usd",
            "qcew_avg_weekly_wage",
            "qcew_avg_wkly_wage_usd_amt",
        ],
    },
    "naics": {
        "naics2_sector_cd": ["naics2_sector_cd", "naics2", "naics2_cd"],
        "naics2_sector_name": [
            "naics2_sector_name",
            "sector_name",
            "sector_label",
            "naics2_name",
            "naics2_sector_desc",
        ],
    },
    "county": {
        "state_cnty_fips_cd": ["state_cnty_fips_cd", "fips", "county_fips"],
        "state_fips": ["state_fips", "state_fips_cd"],
        "county_fips": ["county_fips", "county_fips_cd"],
        "county_name": ["county_name", "cnty_name", "cnty_nm"],
        "state_abbr": ["state_abbr", "state_cd"],
    },
}

EXPECTED_YEARS = {2022, 2023}
EXPECTED_COUNTIES = 3283
EXPECTED_NAICS2 = 20


def add_check(results, name, severity, passed, detail):
    results.append(
        {
            "name": name,
            "severity": severity,
            "passed": passed,
            "detail": detail,
        }
    )


def resolve_columns(df, mapping):
    resolved = {}
    missing = []
    for canonical, options in mapping.items():
        found = next((col for col in options if col in df.columns), None)
        if found is None:
            missing.append(canonical)
        else:
            resolved[canonical] = found
    return resolved, missing


def read_csv_checked(path, label, results):
    if not os.path.exists(path):
        add_check(results, f"{label}: file exists", "ERROR", False, f"Missing file: {path}")
        return None
    if os.path.getsize(path) == 0:
        add_check(results, f"{label}: file non-empty", "ERROR", False, f"Empty file: {path}")
        return None

    try:
        df = pd.read_csv(path, dtype=str, low_memory=False, on_bad_lines="error")
    except Exception as exc:
        add_check(results, f"{label}: CSV parses cleanly", "ERROR", False, f"Parse error: {exc}")
        return None

    add_check(results, f"{label}: CSV parses cleanly", "ERROR", True, "Parsed without malformed rows.")

    if df.columns.empty or len(df.columns) == 1:
        add_check(
            results,
            f"{label}: delimiter sanity",
            "ERROR",
            False,
            f"Parsed {len(df.columns)} column(s); possible delimiter issue.",
        )
    else:
        add_check(results, f"{label}: delimiter sanity", "ERROR", True, f"Parsed {len(df.columns)} columns.")

    if any(col is None or str(col).strip() == "" for col in df.columns):
        add_check(results, f"{label}: non-empty headers", "ERROR", False, "Found empty column name(s).")
    else:
        add_check(results, f"{label}: non-empty headers", "ERROR", True, "All column names present.")

    if len(set(df.columns)) != len(df.columns):
        add_check(results, f"{label}: unique headers", "ERROR", False, "Duplicate column names found.")
    else:
        add_check(results, f"{label}: unique headers", "ERROR", True, "Column names are unique.")

    return df


def numeric_stats(series):
    original = series.copy()
    numeric = pd.to_numeric(series, errors="coerce")
    total = len(series)
    nulls = numeric.isna().sum()
    non_numeric = original.notna() & original.astype(str).str.strip().ne("") & numeric.isna()
    return numeric, {
        "null_pct": (nulls / total * 100) if total else 0.0,
        "non_numeric_pct": (non_numeric.sum() / total * 100) if total else 0.0,
    }


def detect_scientific(series):
    mask = series.astype(str).str.contains(r"[eE][+-]?\d+", regex=True, na=False)
    return int(mask.sum())


def format_table(rows, headers):
    if not rows:
        return "_None_"
    header_line = "| " + " | ".join(headers) + " |"
    separator = "| " + " | ".join(["---"] * len(headers)) + " |"
    body = "\n".join("| " + " | ".join(str(row.get(h, "")) for h in headers) + " |" for row in rows)
    return "\n".join([header_line, separator, body])


def main():
    parser = argparse.ArgumentParser(description="Offline sanity checks for exported CSVs.")
    parser.add_argument("--fact", required=True, help="Path to fact export CSV.")
    parser.add_argument("--naics", required=True, help="Path to NAICS reference CSV.")
    parser.add_argument("--county", required=True, help="Path to county reference CSV.")
    parser.add_argument("--outdir", required=True, help="Output directory for reports.")
    args = parser.parse_args()

    results = []
    run_ts = datetime.now()
    run_id = run_ts.strftime("%Y%m%d_%H%M%S")

    fact_df = read_csv_checked(args.fact, "fact", results)
    naics_df = read_csv_checked(args.naics, "naics", results)
    county_df = read_csv_checked(args.county, "county", results)

    if fact_df is None or naics_df is None or county_df is None:
        error_count = sum(1 for r in results if r["severity"] == "ERROR" and not r["passed"])
        print(f"Sanity checks failed early. Errors: {error_count}")
        sys.exit(1)

    fact_cols, missing_fact = resolve_columns(fact_df, COLUMN_MAP["fact"])
    if missing_fact:
        add_check(
            results,
            "fact: required columns",
            "ERROR",
            False,
            f"Missing columns: {', '.join(missing_fact)}",
        )
    else:
        add_check(results, "fact: required columns", "ERROR", True, "All required columns present.")

    naics_cols, missing_naics = resolve_columns(naics_df, COLUMN_MAP["naics"])
    if missing_naics:
        add_check(
            results,
            "naics: required columns",
            "ERROR",
            False,
            f"Missing columns: {', '.join(missing_naics)}",
        )
    else:
        add_check(results, "naics: required columns", "ERROR", True, "All required columns present.")

    county_cols, missing_county = resolve_columns(county_df, COLUMN_MAP["county"])
    needed_county = ["state_cnty_fips_cd", "county_name", "state_abbr"]
    missing_required = [col for col in needed_county if col not in county_cols]
    if missing_required:
        add_check(
            results,
            "county: required columns",
            "ERROR",
            False,
            f"Missing columns: {', '.join(missing_required)}",
        )
    else:
        add_check(results, "county: required columns", "ERROR", True, "All required columns present.")

    if "state_cnty_fips_cd" not in county_cols and (
        "state_fips" in county_cols and "county_fips" in county_cols
    ):
        county_df["_state_cnty_fips_cd"] = (
            county_df[county_cols["state_fips"]].str.zfill(2)
            + county_df[county_cols["county_fips"]].str.zfill(3)
        )
        county_cols["state_cnty_fips_cd"] = "_state_cnty_fips_cd"

    if "state_cnty_fips_cd" not in county_cols:
        add_check(
            results,
            "county: state_cnty_fips_cd available",
            "ERROR",
            False,
            "Missing state_cnty_fips_cd or derivable state_fips + county_fips.",
        )

    if "state_cnty_fips_cd" in fact_cols:
        fips_series = fact_df[fact_cols["state_cnty_fips_cd"]].astype(str)
        bad_len = (~fips_series.str.fullmatch(r"\d{5}")).sum()
        if bad_len:
            add_check(
                results,
                "fact: state_cnty_fips_cd format",
                "ERROR",
                False,
                f"{bad_len} rows have invalid FIPS (expected 5 digits).",
            )
        else:
            add_check(results, "fact: state_cnty_fips_cd format", "ERROR", True, "All FIPS codes are 5 digits.")

    year_stats = {}
    numeric_summary = {}
    scientific_counts = {}
    null_rates_by_year = {}

    if all(col in fact_cols for col in ("year_num", "naics2_sector_cd")):
        year_series = fact_df[fact_cols["year_num"]]
        year_numeric, year_stat = numeric_stats(year_series)
        if year_numeric.isna().any():
            add_check(
                results,
                "fact: year_num parse",
                "ERROR",
                False,
                f"{int(year_numeric.isna().sum())} rows have non-numeric year_num.",
            )
        else:
            add_check(results, "fact: year_num parse", "ERROR", True, "All year_num values parse numeric.")

        if (year_numeric.dropna() % 1 != 0).any():
            add_check(results, "fact: year_num integer", "ERROR", False, "Non-integer year_num values found.")
        else:
            add_check(results, "fact: year_num integer", "ERROR", True, "All year_num values are integers.")

        year_values = set(year_numeric.dropna().astype(int).unique())
        unexpected_years = sorted(year_values - EXPECTED_YEARS)
        if unexpected_years:
            add_check(
                results,
                "fact: year_num expected set",
                "WARN",
                False,
                f"Unexpected years present: {unexpected_years}",
            )
        else:
            add_check(results, "fact: year_num expected set", "WARN", True, "Only expected years present.")

        fact_df["_year_num_int"] = year_numeric.astype("Int64")

        naics_series = fact_df[fact_cols["naics2_sector_cd"]].astype(str)
        if naics_series.isna().any() or (naics_series.str.strip() == "").any():
            add_check(results, "fact: naics2_sector_cd non-null", "ERROR", False, "Null NAICS2 codes found.")
        else:
            add_check(results, "fact: naics2_sector_cd non-null", "ERROR", True, "NAICS2 codes present.")

    fact_numeric_cols = [
        "abs_firms",
        "abs_emp",
        "abs_payroll_usd_amt",
        "abs_rcpt_usd_amt",
        "qcew_emp",
        "qcew_wages_usd",
        "qcew_avg_weekly_wage_usd",
    ]

    for col in fact_numeric_cols:
        if col not in fact_cols:
            continue
        series = fact_df[fact_cols[col]]
        numeric, stats = numeric_stats(series)
        fact_df[f"_{col}_num"] = numeric
        numeric_summary[col] = stats
        scientific_counts[col] = detect_scientific(series)

    if numeric_summary:
        add_check(results, "fact: numeric parse summary", "WARN", True, "Numeric columns parsed (see report).")

    if "naics2_sector_cd" in fact_cols and "naics2_sector_cd" in naics_cols:
        expected_naics = set(naics_df[naics_cols["naics2_sector_cd"]].astype(str).dropna().unique())
        fact_naics = set(fact_df[fact_cols["naics2_sector_cd"]].astype(str).dropna().unique())
        unexpected_naics = sorted(fact_naics - expected_naics)
        if unexpected_naics:
            add_check(
                results,
                "fact: naics2_sector_cd set",
                "WARN",
                False,
                f"Unexpected NAICS2 codes: {unexpected_naics[:20]}{'...' if len(unexpected_naics) > 20 else ''}",
            )
        else:
            add_check(results, "fact: naics2_sector_cd set", "WARN", True, "NAICS2 codes match reference.")

    key_cols = [fact_cols.get("year_num"), fact_cols.get("state_cnty_fips_cd"), fact_cols.get("naics2_sector_cd")]
    if all(key_cols):
        dup_counts = fact_df.groupby(key_cols).size()
        dup_keys = dup_counts[dup_counts > 1].sort_values(ascending=False)
        if not dup_keys.empty:
            add_check(
                results,
                "fact: duplicate keys",
                "ERROR",
                False,
                f"{len(dup_keys)} duplicate keys found.",
            )
        else:
            add_check(results, "fact: duplicate keys", "ERROR", True, "No duplicate keys.")
    else:
        dup_keys = pd.Series(dtype=int)

    if "state_cnty_fips_cd" in fact_cols and "state_cnty_fips_cd" in county_cols:
        fact_fips = set(fact_df[fact_cols["state_cnty_fips_cd"]].astype(str).dropna().unique())
        county_fips = set(county_df[county_cols["state_cnty_fips_cd"]].astype(str).dropna().unique())
        missing_fips = sorted(fact_fips - county_fips)
        if missing_fips:
            add_check(
                results,
                "join: fact -> county",
                "ERROR",
                False,
                f"{len(missing_fips)} fact FIPS missing in county ref.",
            )
        else:
            add_check(results, "join: fact -> county", "ERROR", True, "All fact FIPS found in county ref.")

        extra_fips = sorted(county_fips - fact_fips)
        if extra_fips:
            add_check(
                results,
                "join: county extra keys",
                "WARN",
                False,
                f"{len(extra_fips)} county keys unused by fact.",
            )
        else:
            add_check(results, "join: county extra keys", "WARN", True, "All county keys used by fact.")

    if "naics2_sector_cd" in fact_cols and "naics2_sector_cd" in naics_cols:
        fact_naics = set(fact_df[fact_cols["naics2_sector_cd"]].astype(str).dropna().unique())
        ref_naics = set(naics_df[naics_cols["naics2_sector_cd"]].astype(str).dropna().unique())
        missing_naics = sorted(fact_naics - ref_naics)
        if missing_naics:
            add_check(
                results,
                "join: fact -> naics",
                "ERROR",
                False,
                f"{len(missing_naics)} fact NAICS codes missing in naics ref.",
            )
        else:
            add_check(results, "join: fact -> naics", "ERROR", True, "All fact NAICS codes in naics ref.")

        extra_naics = sorted(ref_naics - fact_naics)
        if extra_naics:
            add_check(
                results,
                "join: naics extra keys",
                "WARN",
                False,
                f"{len(extra_naics)} naics keys unused by fact.",
            )
        else:
            add_check(results, "join: naics extra keys", "WARN", True, "All naics keys used by fact.")

    if "_year_num_int" in fact_df:
        year_counts = (
            fact_df.groupby("_year_num_int").size().dropna().astype(int).to_dict()
        )
        year_stats["rows_by_year"] = year_counts
        distinct_counties = (
            fact_df.groupby("_year_num_int")[fact_cols["state_cnty_fips_cd"]]
            .nunique()
            .dropna()
            .astype(int)
            .to_dict()
        )
        distinct_naics = (
            fact_df.groupby("_year_num_int")[fact_cols["naics2_sector_cd"]]
            .nunique()
            .dropna()
            .astype(int)
            .to_dict()
        )

        for year, cnt in distinct_counties.items():
            if cnt != EXPECTED_COUNTIES:
                add_check(
                    results,
                    f"coverage: distinct counties {year}",
                    "WARN",
                    False,
                    f"{cnt} counties (expected {EXPECTED_COUNTIES}).",
                )
            else:
                add_check(
                    results,
                    f"coverage: distinct counties {year}",
                    "WARN",
                    True,
                    f"{cnt} counties (expected {EXPECTED_COUNTIES}).",
                )

        for year, cnt in distinct_naics.items():
            if cnt != EXPECTED_NAICS2:
                add_check(
                    results,
                    f"coverage: distinct naics2 {year}",
                    "WARN",
                    False,
                    f"{cnt} NAICS2 codes (expected {EXPECTED_NAICS2}).",
                )
            else:
                add_check(
                    results,
                    f"coverage: distinct naics2 {year}",
                    "WARN",
                    True,
                    f"{cnt} NAICS2 codes (expected {EXPECTED_NAICS2}).",
                )

        if "abs_firms" in fact_cols and "_abs_firms_num" in fact_df:
            abs_null_pct = (
                fact_df.groupby("_year_num_int")["_abs_firms_num"]
                .apply(lambda s: s.isna().mean() * 100)
                .to_dict()
            )
            null_rates_by_year["abs_firms_null_pct"] = abs_null_pct

        if "qcew_emp" in fact_cols and "_qcew_emp_num" in fact_df:
            qcew_null_pct = (
                fact_df.groupby("_year_num_int")["_qcew_emp_num"]
                .apply(lambda s: s.isna().mean() * 100)
                .to_dict()
            )
            null_rates_by_year["qcew_emp_null_pct"] = qcew_null_pct

    abs_cols = ["abs_firms", "abs_emp", "abs_payroll_usd_amt", "abs_rcpt_usd_amt"]
    if all(col in fact_df.columns for col in [f"_{c}_num" for c in abs_cols]):
        abs_numeric = fact_df[[f"_{c}_num" for c in abs_cols]]
        abs_any = abs_numeric.notna().any(axis=1)
        abs_partial = abs_any & abs_numeric.isna().any(axis=1)
        partial_count = int(abs_partial.sum())
        if partial_count:
            add_check(
                results,
                "coverage: partial ABS rows",
                "WARN",
                False,
                f"{partial_count} rows have partial ABS values.",
            )
        else:
            add_check(results, "coverage: partial ABS rows", "WARN", True, "No partial ABS rows.")

    negative_errors = 0
    negative_warns = 0
    error_metrics = ["abs_firms", "abs_emp", "qcew_emp"]
    warn_metrics = ["abs_payroll_usd_amt", "abs_rcpt_usd_amt", "qcew_wages_usd", "qcew_avg_weekly_wage_usd"]

    for metric in error_metrics:
        num_col = f"_{metric}_num"
        if num_col in fact_df:
            count = int((fact_df[num_col] < 0).sum())
            if count:
                negative_errors += count

    if negative_errors:
        add_check(
            results,
            "fact: negative firms/emp",
            "ERROR",
            False,
            f"{negative_errors} rows have negative firm/emp values.",
        )
    else:
        add_check(results, "fact: negative firms/emp", "ERROR", True, "No negative firm/emp values.")

    for metric in warn_metrics:
        num_col = f"_{metric}_num"
        if num_col in fact_df:
            count = int((fact_df[num_col] < 0).sum())
            if count:
                negative_warns += count

    if negative_warns:
        add_check(
            results,
            "fact: negative dollar values",
            "WARN",
            False,
            f"{negative_warns} rows have negative dollar values.",
        )
    else:
        add_check(results, "fact: negative dollar values", "WARN", True, "No negative dollar values.")

    sci_total = sum(scientific_counts.values()) if scientific_counts else 0
    if sci_total:
        add_check(
            results,
            "formatting: scientific notation strings",
            "WARN",
            False,
            f"{sci_total} numeric strings appear in scientific notation.",
        )
    else:
        add_check(results, "formatting: scientific notation strings", "WARN", True, "No scientific notation strings.")

    outdir = args.outdir
    os.makedirs(outdir, exist_ok=True)
    report_path = os.path.join(outdir, f"export_sanity_report_{run_id}.md")
    json_path = os.path.join(outdir, f"export_sanity_report_{run_id}.json")

    error_fails = [r for r in results if r["severity"] == "ERROR" and not r["passed"]]
    warn_fails = [r for r in results if r["severity"] == "WARN" and not r["passed"]]
    error_pass = [r for r in results if r["severity"] == "ERROR" and r["passed"]]
    warn_pass = [r for r in results if r["severity"] == "WARN" and r["passed"]]

    print("Export sanity check summary")
    print(f"Errors: {len(error_fails)} failed, {len(error_pass)} passed")
    print(f"Warnings: {len(warn_fails)} failed, {len(warn_pass)} passed")
    print(f"Report: {report_path}")

    summary_rows = [
        {"Severity": "ERROR", "Passed": len(error_pass), "Failed": len(error_fails)},
        {"Severity": "WARN", "Passed": len(warn_pass), "Failed": len(warn_fails)},
    ]

    key_stats = {
        "rows": len(fact_df),
        "years": sorted(fact_df["_year_num_int"].dropna().unique().tolist())
        if "_year_num_int" in fact_df
        else [],
        "distinct_counties_by_year": year_stats.get("rows_by_year", {}),
    }

    dup_rows = []
    if dup_keys is not None and not dup_keys.empty:
        dup_keys = dup_keys.reset_index().rename(columns={0: "row_count"})
        dup_keys = dup_keys.head(50)
        for _, row in dup_keys.iterrows():
            dup_rows.append(
                {
                    "year_num": row[key_cols[0]],
                    "state_cnty_fips_cd": row[key_cols[1]],
                    "naics2_sector_cd": row[key_cols[2]],
                    "row_count": int(row["row_count"]),
                }
            )

    null_rows = []
    for metric, stats in numeric_summary.items():
        null_rows.append(
            {
                "metric": metric,
                "null_pct": f"{stats['null_pct']:.2f}",
                "non_numeric_pct": f"{stats['non_numeric_pct']:.2f}",
                "scientific_strings": scientific_counts.get(metric, 0),
            }
        )

    outlier_sections = []
    outlier_metrics = ["abs_rcpt_usd_amt", "abs_payroll_usd_amt", "qcew_wages_usd"]
    for metric in outlier_metrics:
        num_col = f"_{metric}_num"
        if num_col not in fact_df or "_year_num_int" not in fact_df:
            continue
        metric_rows = []
        for year in sorted(fact_df["_year_num_int"].dropna().unique()):
            subset = fact_df[fact_df["_year_num_int"] == year]
            top = subset.nlargest(10, num_col)
            for _, row in top.iterrows():
                metric_rows.append(
                    {
                        "year": int(year),
                        "state_cnty_fips_cd": row[fact_cols["state_cnty_fips_cd"]],
                        "naics2_sector_cd": row[fact_cols["naics2_sector_cd"]],
                        "value": row[num_col],
                    }
                )
        outlier_sections.append(
            {
                "metric": metric,
                "table": format_table(
                    metric_rows,
                    ["year", "state_cnty_fips_cd", "naics2_sector_cd", "value"],
                ),
            }
        )
    add_check(results, "outliers: sample top 10", "WARN", True, "Outliers sampled in report.")

    report_lines = []
    report_lines.append(f"# Export Sanity Report ({run_ts.isoformat(timespec='seconds')})")
    report_lines.append("")
    report_lines.append("## Inputs")
    report_lines.append(f"- Fact: `{args.fact}`")
    report_lines.append(f"- NAICS ref: `{args.naics}`")
    report_lines.append(f"- County ref: `{args.county}`")
    report_lines.append(f"- Run timestamp: `{run_ts.isoformat(timespec='seconds')}`")
    report_lines.append("")
    report_lines.append("## Summary")
    report_lines.append(format_table(summary_rows, ["Severity", "Passed", "Failed"]))
    report_lines.append("")
    report_lines.append("## Key stats")
    report_lines.append(f"- Rows: {len(fact_df)}")
    if "_year_num_int" in fact_df:
        report_lines.append(f"- Years: {sorted(fact_df['_year_num_int'].dropna().unique().tolist())}")
        report_lines.append(f"- Rows by year: {year_stats.get('rows_by_year', {})}")
    if "_year_num_int" in fact_df and "state_cnty_fips_cd" in fact_cols:
        year_counts = (
            fact_df.groupby("_year_num_int")[fact_cols["state_cnty_fips_cd"]].nunique().to_dict()
        )
        report_lines.append(f"- Distinct counties by year: {year_counts}")
    if "_year_num_int" in fact_df and "naics2_sector_cd" in fact_cols:
        naics_counts = (
            fact_df.groupby("_year_num_int")[fact_cols["naics2_sector_cd"]].nunique().to_dict()
        )
        report_lines.append(f"- Distinct NAICS2 by year: {naics_counts}")
    if null_rates_by_year:
        report_lines.append(f"- ABS firms null % by year: {null_rates_by_year.get('abs_firms_null_pct', {})}")
        report_lines.append(f"- QCEW emp null % by year: {null_rates_by_year.get('qcew_emp_null_pct', {})}")
    report_lines.append("")
    report_lines.append("## Duplicate keys")
    report_lines.append(format_table(dup_rows, ["year_num", "state_cnty_fips_cd", "naics2_sector_cd", "row_count"]))
    report_lines.append("")
    report_lines.append("## Missing joins")
    missing_join_rows = []
    for result in results:
        if result["severity"] in {"ERROR", "WARN"} and "join:" in result["name"] and not result["passed"]:
            missing_join_rows.append({"check": result["name"], "detail": result["detail"]})
    report_lines.append(format_table(missing_join_rows, ["check", "detail"]))
    report_lines.append("")
    report_lines.append("## Null-rate table (numeric columns)")
    report_lines.append(format_table(null_rows, ["metric", "null_pct", "non_numeric_pct", "scientific_strings"]))
    report_lines.append("")
    report_lines.append("## Sample outliers (top 10 by year)")
    for section in outlier_sections:
        report_lines.append(f"### {section['metric']}")
        report_lines.append(section["table"])
        report_lines.append("")

    report_lines.append("## Check details")
    detail_rows = [
        {"severity": r["severity"], "check": r["name"], "status": "PASS" if r["passed"] else "FAIL", "detail": r["detail"]}
        for r in results
    ]
    report_lines.append(format_table(detail_rows, ["severity", "check", "status", "detail"]))

    with open(report_path, "w", encoding="utf-8") as handle:
        handle.write("\n".join(report_lines).strip() + "\n")

    json_payload = {
        "run_timestamp": run_ts.isoformat(timespec="seconds"),
        "inputs": {
            "fact": args.fact,
            "naics": args.naics,
            "county": args.county,
        },
        "summary": {
            "error_passed": len(error_pass),
            "error_failed": len(error_fails),
            "warn_passed": len(warn_pass),
            "warn_failed": len(warn_fails),
        },
        "checks": results,
    }

    with open(json_path, "w", encoding="utf-8") as handle:
        json.dump(json_payload, handle, indent=2)

    if error_fails:
        sys.exit(1)


if __name__ == "__main__":
    main()
