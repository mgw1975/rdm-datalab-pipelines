#!/usr/bin/env python3
import argparse
import csv
import glob
import os
from datetime import datetime

import pandas as pd

REQUIRED_FIELDS = [
    "file_name",
    "column_name",
    "description",
    "source_system",
    "unit",
    "additive",
    "expected_nulls",
    "null_reason",
    "notes",
]

SOURCE_ABS = "ABS"
SOURCE_QCEW = "QCEW"
SOURCE_CENSUS = "Census reference"
SOURCE_INTERNAL = "Internal export"


def collect_files(pattern):
    matches = sorted(glob.glob(pattern))
    if matches:
        return matches
    if os.path.exists(pattern):
        return [pattern]
    return []


def base_attributes(column_name, file_type):
    notes = ""
    expected_nulls = "No"
    null_reason = ""

    if column_name in {"year_num", "state_cnty_fips_cd", "naics2_sector_cd"}:
        return {
            "description": {
                "year_num": "Calendar year (integer).",
                "state_cnty_fips_cd": "5-digit county FIPS code (state + county).",
                "naics2_sector_cd": "NAICS 2-digit sector code (includes hyphenated sectors).",
            }[column_name],
            "source_system": SOURCE_INTERNAL,
            "unit": "count" if column_name == "year_num" else "code",
            "additive": "No (dimension key).",
            "expected_nulls": "No",
            "null_reason": "",
            "notes": "",
        }

    if column_name.endswith("_cd") or "fips" in column_name or "geo_id" in column_name:
        return {
            "description": "Geographic or sector code.",
            "source_system": SOURCE_CENSUS if file_type != "fact" else SOURCE_INTERNAL,
            "unit": "code",
            "additive": "No (dimension key).",
            "expected_nulls": "No",
            "null_reason": "",
            "notes": "",
        }

    if any(token in column_name for token in ("_nm", "_desc", "_name")):
        return {
            "description": "Descriptive label.",
            "source_system": SOURCE_CENSUS,
            "unit": "string",
            "additive": "No (label).",
            "expected_nulls": "No",
            "null_reason": "",
            "notes": "",
        }

    if "abs_" in column_name:
        expected_nulls = "Yes"
        null_reason = "ABS not published for all county×sector keys."

    if "qcew_" in column_name:
        expected_nulls = "Yes"
        null_reason = "QCEW suppression or non-covered cells."

    unit = "count"
    additive = "Yes"
    source_system = SOURCE_INTERNAL

    if "abs_" in column_name:
        source_system = SOURCE_ABS
    if "qcew_" in column_name:
        source_system = SOURCE_QCEW

    if "_usd_amt" in column_name or "wage" in column_name or "payroll" in column_name or "rcpt" in column_name:
        unit = "USD (nominal)"

    if any(token in column_name for token in ("avg", "per_", "_per_", "idx", "rank")):
        additive = "No (ratio/index)."
    if column_name.startswith(("state_", "cnty_")):
        additive = "No (repeated across rows)."

    if column_name.endswith("_num"):
        unit = unit

    return {
        "description": "Metric value.",
        "source_system": source_system,
        "unit": unit,
        "additive": additive if additive != "Yes" else "Yes",
        "expected_nulls": expected_nulls,
        "null_reason": null_reason,
        "notes": "",
    }


def build_fact_overrides():
    return {
        "state_cnty_fips_cd": (
            "5-digit county FIPS code (state + county).",
            SOURCE_CENSUS,
            "code",
            "No (dimension key).",
            "No",
            "",
            "",
        ),
        "naics2_sector_cd": (
            "NAICS 2-digit sector code (includes hyphenated sectors).",
            SOURCE_CENSUS,
            "code",
            "No (dimension key).",
            "No",
            "",
            "",
        ),
        "year_num": (
            "Calendar year (integer).",
            SOURCE_INTERNAL,
            "count",
            "No (dimension key).",
            "No",
            "",
            "",
        ),
        "naics2_sector_desc": (
            "NAICS 2-digit sector name.",
            SOURCE_CENSUS,
            "string",
            "No (label).",
            "No",
            "",
            "",
        ),
        "cnty_nm": (
            "County name.",
            SOURCE_CENSUS,
            "string",
            "No (label).",
            "No",
            "",
            "",
        ),
        "state_nm": (
            "State name.",
            SOURCE_CENSUS,
            "string",
            "No (label).",
            "No",
            "",
            "",
        ),
        "cnty_full_nm": (
            "County + state name (combined label).",
            SOURCE_CENSUS,
            "string",
            "No (label).",
            "No",
            "",
            "",
        ),
        "population_num": (
            "County population (reference year).",
            SOURCE_CENSUS,
            "count",
            "No (repeated across rows).",
            "No",
            "",
            "",
        ),
        "population_year": (
            "Population reference year.",
            SOURCE_CENSUS,
            "count",
            "No (dimension key).",
            "No",
            "",
            "",
        ),
        "geo_id": (
            "Geographic identifier from Census reference.",
            SOURCE_CENSUS,
            "code",
            "No (dimension key).",
            "No",
            "",
            "",
        ),
        "ind_level_num": (
            "Industry level indicator (NAICS aggregation level).",
            SOURCE_INTERNAL,
            "count",
            "No (classification flag).",
            "No",
            "",
            "",
        ),
        "abs_firm_num": (
            "ABS employer firms.",
            SOURCE_ABS,
            "count",
            "Yes",
            "Yes",
            "ABS not published for all county×sector keys.",
            "",
        ),
        "abs_firm_prev_year_num": (
            "ABS employer firms (previous year).",
            SOURCE_ABS,
            "count",
            "Yes",
            "Yes",
            "ABS not published for all county×sector keys.",
            "",
        ),
        "abs_emp_num": (
            "ABS employment (employer firms).",
            SOURCE_ABS,
            "count",
            "Yes",
            "Yes",
            "ABS not published for all county×sector keys.",
            "",
        ),
        "abs_emp_prev_year_num": (
            "ABS employment (previous year).",
            SOURCE_ABS,
            "count",
            "Yes",
            "Yes",
            "ABS not published for all county×sector keys.",
            "",
        ),
        "abs_payroll_usd_amt": (
            "ABS payroll (nominal USD).",
            SOURCE_ABS,
            "USD (nominal)",
            "Yes",
            "Yes",
            "ABS not published for all county×sector keys.",
            "",
        ),
        "abs_payroll_prev_year_usd_amt": (
            "ABS payroll (previous year, nominal USD).",
            SOURCE_ABS,
            "USD (nominal)",
            "Yes",
            "Yes",
            "ABS not published for all county×sector keys.",
            "",
        ),
        "abs_rcpt_usd_amt": (
            "ABS receipts (nominal USD).",
            SOURCE_ABS,
            "USD (nominal)",
            "Yes",
            "Yes",
            "ABS not published for all county×sector keys.",
            "",
        ),
        "abs_rcpt_prev_year_usd_amt": (
            "ABS receipts (previous year, nominal USD).",
            SOURCE_ABS,
            "USD (nominal)",
            "Yes",
            "Yes",
            "ABS not published for all county×sector keys.",
            "",
        ),
        "abs_rcpt_per_emp_usd_amt": (
            "ABS receipts per employee (nominal USD).",
            SOURCE_ABS,
            "USD (nominal)",
            "No (ratio).",
            "Yes",
            "ABS not published for all county×sector keys.",
            "",
        ),
        "cnty_abs_firm_num": (
            "County-level ABS employer firm total (all sectors).",
            SOURCE_ABS,
            "count",
            "No (repeated across rows).",
            "Yes",
            "ABS not published for all counties.",
            "",
        ),
        "cnty_abs_firm_prev_year_num": (
            "County-level ABS employer firm total (previous year).",
            SOURCE_ABS,
            "count",
            "No (repeated across rows).",
            "Yes",
            "ABS not published for all counties.",
            "",
        ),
        "cnty_abs_rcpt_usd_amt": (
            "County-level ABS receipts total (all sectors, nominal USD).",
            SOURCE_ABS,
            "USD (nominal)",
            "No (repeated across rows).",
            "Yes",
            "ABS not published for all counties.",
            "",
        ),
        "cnty_firm_cncntrtn_idx": (
            "County firm concentration index (higher = more concentrated).",
            SOURCE_INTERNAL,
            "count",
            "No (index).",
            "Yes",
            "Derived metric may be null when base values are missing.",
            "",
        ),
        "qcew_ann_avg_emp_lvl_num": (
            "QCEW annual average employment level.",
            SOURCE_QCEW,
            "count",
            "Yes",
            "Yes",
            "QCEW suppression or non-covered cells.",
            "",
        ),
        "qcew_ann_avg_emp_prev_year_num": (
            "QCEW annual average employment (previous year).",
            SOURCE_QCEW,
            "count",
            "Yes",
            "Yes",
            "QCEW suppression or non-covered cells.",
            "",
        ),
        "qcew_ttl_ann_wage_usd_amt": (
            "QCEW total annual wages (nominal USD).",
            SOURCE_QCEW,
            "USD (nominal)",
            "Yes",
            "Yes",
            "QCEW suppression or non-covered cells.",
            "",
        ),
        "qcew_ttl_ann_wage_prev_year_usd_amt": (
            "QCEW total annual wages (previous year, nominal USD).",
            SOURCE_QCEW,
            "USD (nominal)",
            "Yes",
            "Yes",
            "QCEW suppression or non-covered cells.",
            "",
        ),
        "qcew_avg_wkly_wage_usd_amt": (
            "QCEW average weekly wage (nominal USD).",
            SOURCE_QCEW,
            "USD (nominal)",
            "No (average).",
            "Yes",
            "QCEW suppression or non-covered cells.",
            "",
        ),
        "qcew_wage_per_emp_usd_amt": (
            "QCEW wages per employee (nominal USD).",
            SOURCE_QCEW,
            "USD (nominal)",
            "No (ratio).",
            "Yes",
            "QCEW suppression or non-covered cells.",
            "",
        ),
        "abs_wage_per_emp_usd_amt": (
            "ABS payroll per employee (nominal USD).",
            SOURCE_ABS,
            "USD (nominal)",
            "No (ratio).",
            "Yes",
            "ABS not published for all county×sector keys.",
            "",
        ),
        "abs_rcpt_per_firm_usd_amt": (
            "ABS receipts per firm (nominal USD).",
            SOURCE_ABS,
            "USD (nominal)",
            "No (ratio).",
            "Yes",
            "ABS not published for all county×sector keys.",
            "",
        ),
        "state_abs_firm_num": (
            "State-level ABS employer firm total (all sectors).",
            SOURCE_ABS,
            "count",
            "No (repeated across rows).",
            "Yes",
            "ABS not published for all states.",
            "",
        ),
        "state_abs_firm_prev_year_num": (
            "State-level ABS employer firm total (previous year).",
            SOURCE_ABS,
            "count",
            "No (repeated across rows).",
            "Yes",
            "ABS not published for all states.",
            "",
        ),
        "state_abs_emp_num": (
            "State-level ABS employment total (all sectors).",
            SOURCE_ABS,
            "count",
            "No (repeated across rows).",
            "Yes",
            "ABS not published for all states.",
            "",
        ),
        "state_abs_emp_prev_year_num": (
            "State-level ABS employment total (previous year).",
            SOURCE_ABS,
            "count",
            "No (repeated across rows).",
            "Yes",
            "ABS not published for all states.",
            "",
        ),
        "state_abs_payroll_usd_amt": (
            "State-level ABS payroll total (nominal USD).",
            SOURCE_ABS,
            "USD (nominal)",
            "No (repeated across rows).",
            "Yes",
            "ABS not published for all states.",
            "",
        ),
        "state_abs_payroll_prev_year_usd_amt": (
            "State-level ABS payroll total (previous year, nominal USD).",
            SOURCE_ABS,
            "USD (nominal)",
            "No (repeated across rows).",
            "Yes",
            "ABS not published for all states.",
            "",
        ),
        "state_abs_rcpt_usd_amt": (
            "State-level ABS receipts total (nominal USD).",
            SOURCE_ABS,
            "USD (nominal)",
            "No (repeated across rows).",
            "Yes",
            "ABS not published for all states.",
            "",
        ),
        "state_abs_rcpt_prev_year_usd_amt": (
            "State-level ABS receipts total (previous year, nominal USD).",
            SOURCE_ABS,
            "USD (nominal)",
            "No (repeated across rows).",
            "Yes",
            "ABS not published for all states.",
            "",
        ),
        "state_qcew_ann_avg_emp_lvl_num": (
            "State-level QCEW annual average employment (all sectors).",
            SOURCE_QCEW,
            "count",
            "No (repeated across rows).",
            "Yes",
            "QCEW suppression or non-covered cells.",
            "",
        ),
        "state_qcew_ann_avg_emp_prev_year_num": (
            "State-level QCEW annual average employment (previous year).",
            SOURCE_QCEW,
            "count",
            "No (repeated across rows).",
            "Yes",
            "QCEW suppression or non-covered cells.",
            "",
        ),
        "state_qcew_ttl_ann_wage_usd_amt": (
            "State-level QCEW total annual wages (nominal USD).",
            SOURCE_QCEW,
            "USD (nominal)",
            "No (repeated across rows).",
            "Yes",
            "QCEW suppression or non-covered cells.",
            "",
        ),
        "state_qcew_ttl_ann_wage_prev_year_usd_amt": (
            "State-level QCEW total annual wages (previous year, nominal USD).",
            SOURCE_QCEW,
            "USD (nominal)",
            "No (repeated across rows).",
            "Yes",
            "QCEW suppression or non-covered cells.",
            "",
        ),
        "state_abs_firm_rank_num": (
            "County rank within state × year × NAICS2 by ABS employer firms (1 = highest).",
            SOURCE_INTERNAL,
            "count",
            "No (rank).",
            "No",
            "",
            "",
        ),
        "state_abs_emp_rank_num": (
            "County rank within state × year × NAICS2 by ABS employment (1 = highest).",
            SOURCE_INTERNAL,
            "count",
            "No (rank).",
            "No",
            "",
            "",
        ),
        "state_abs_payroll_rank_num": (
            "County rank within state × year × NAICS2 by ABS payroll (1 = highest).",
            SOURCE_INTERNAL,
            "count",
            "No (rank).",
            "No",
            "",
            "",
        ),
        "state_abs_rcpt_rank_num": (
            "County rank within state × year × NAICS2 by ABS receipts (1 = highest).",
            SOURCE_INTERNAL,
            "count",
            "No (rank).",
            "No",
            "",
            "",
        ),
        "state_qcew_emp_rank_num": (
            "County rank within state × year × NAICS2 by QCEW employment (1 = highest).",
            SOURCE_INTERNAL,
            "count",
            "No (rank).",
            "No",
            "",
            "",
        ),
        "state_qcew_wage_rank_num": (
            "County rank within state × year × NAICS2 by QCEW wages (1 = highest).",
            SOURCE_INTERNAL,
            "count",
            "No (rank).",
            "No",
            "",
            "",
        ),
    }


def build_naics_overrides():
    return {
        "naics2_sector_cd": (
            "NAICS 2-digit sector code (includes hyphenated sectors).",
            SOURCE_CENSUS,
            "code",
            "No (dimension key).",
            "No",
            "",
            "",
        ),
        "naics2_sector_desc": (
            "NAICS 2-digit sector name.",
            SOURCE_CENSUS,
            "string",
            "No (label).",
            "No",
            "",
            "",
        ),
    }


def build_county_overrides():
    return {
        "state_cnty_fips_cd": (
            "5-digit county FIPS code (state + county).",
            SOURCE_CENSUS,
            "code",
            "No (dimension key).",
            "No",
            "",
            "",
        ),
        "state_cd": (
            "2-letter state abbreviation.",
            SOURCE_CENSUS,
            "code",
            "No (dimension key).",
            "No",
            "",
            "",
        ),
        "cnty_ansi_nm": (
            "County ANSI name.",
            SOURCE_CENSUS,
            "string",
            "No (label).",
            "No",
            "",
            "",
        ),
        "cnty_nm": (
            "County name.",
            SOURCE_CENSUS,
            "string",
            "No (label).",
            "No",
            "",
            "",
        ),
        "land_area_num": (
            "County land area (Census units).",
            SOURCE_CENSUS,
            "count",
            "No (area measure).",
            "No",
            "",
            "",
        ),
        "water_area_num": (
            "County water area (Census units).",
            SOURCE_CENSUS,
            "count",
            "No (area measure).",
            "No",
            "",
            "",
        ),
        "lat_num": (
            "County centroid latitude.",
            SOURCE_CENSUS,
            "count",
            "No (coordinate).",
            "No",
            "",
            "",
        ),
        "long_num": (
            "County centroid longitude.",
            SOURCE_CENSUS,
            "count",
            "No (coordinate).",
            "No",
            "",
            "",
        ),
        "population_num": (
            "County population (reference year).",
            SOURCE_CENSUS,
            "count",
            "Yes",
            "No",
            "",
            "",
        ),
        "population_year": (
            "Population reference year.",
            SOURCE_CENSUS,
            "count",
            "No (dimension key).",
            "No",
            "",
            "",
        ),
    }


def infer_row(column_name, file_type, overrides):
    if column_name in overrides:
        description, source, unit, additive, expected_nulls, null_reason, notes = overrides[column_name]
        return {
            "description": description,
            "source_system": source,
            "unit": unit,
            "additive": additive,
            "expected_nulls": expected_nulls,
            "null_reason": null_reason,
            "notes": notes,
        }

    base = base_attributes(column_name, file_type)
    return base


def build_rows(file_paths, file_type):
    overrides = {
        "fact": build_fact_overrides(),
        "naics": build_naics_overrides(),
        "county": build_county_overrides(),
    }[file_type]

    rows = []
    file_order = []

    for path in file_paths:
        df = pd.read_csv(path, nrows=0)
        file_name = os.path.basename(path)
        file_order.append(file_name)
        for col in df.columns:
            attrs = infer_row(col, file_type, overrides)
            row = {
                "file_name": file_name,
                "column_name": col,
                "description": attrs["description"],
                "source_system": attrs["source_system"],
                "unit": attrs["unit"],
                "additive": attrs["additive"],
                "expected_nulls": attrs["expected_nulls"],
                "null_reason": attrs["null_reason"],
                "notes": attrs["notes"],
            }
            rows.append(row)

    return rows, file_order


def write_markdown(rows_by_file, out_path):
    lines = []
    lines.append("# RDM Datalab v1 Data Dictionary")
    lines.append("")
    lines.append("## Definitions")
    lines.append("- Additive: sums across rows without double-counting (true for raw counts and totals).")
    lines.append("- Expected nulls: nulls are expected for some records due to source limitations or suppression.")
    lines.append("- Nominal USD: values are not inflation-adjusted.")
    lines.append("")

    for file_name, rows in rows_by_file.items():
        lines.append(f"## {file_name}")
        lines.append("")
        header = "| " + " | ".join(REQUIRED_FIELDS) + " |"
        separator = "| " + " | ".join(["---"] * len(REQUIRED_FIELDS)) + " |"
        lines.append(header)
        lines.append(separator)
        for row in rows:
            line = "| " + " | ".join(row[field] if row[field] != "" else " " for field in REQUIRED_FIELDS) + " |"
            lines.append(line)
        lines.append("")

    with open(out_path, "w", encoding="utf-8") as handle:
        handle.write("\n".join(lines).strip() + "\n")


def write_csv(rows, out_path):
    with open(out_path, "w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=REQUIRED_FIELDS)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def main():
    parser = argparse.ArgumentParser(description="Build data dictionary for Gumroad v1 exports.")
    parser.add_argument("--fact_glob", required=True, help="Glob for fact CSV export(s).")
    parser.add_argument("--naics_glob", required=True, help="Glob for NAICS reference CSV export(s).")
    parser.add_argument("--county_glob", required=True, help="Glob for county reference CSV export(s).")
    parser.add_argument("--out_md", required=True, help="Markdown output path.")
    parser.add_argument("--out_csv", required=True, help="CSV output path.")
    args = parser.parse_args()

    fact_files = collect_files(args.fact_glob)
    naics_files = collect_files(args.naics_glob)
    county_files = collect_files(args.county_glob)

    if not fact_files:
        raise SystemExit(f"No fact files found for pattern: {args.fact_glob}")
    if not naics_files:
        raise SystemExit(f"No NAICS files found for pattern: {args.naics_glob}")
    if not county_files:
        raise SystemExit(f"No county files found for pattern: {args.county_glob}")

    fact_rows, fact_order = build_rows(fact_files, "fact")
    naics_rows, naics_order = build_rows(naics_files, "naics")
    county_rows, county_order = build_rows(county_files, "county")

    all_rows = fact_rows + naics_rows + county_rows
    all_rows_sorted = sorted(all_rows, key=lambda r: (r["file_name"], r["column_name"]))

    rows_by_file = {}
    for file_name in fact_order + naics_order + county_order:
        rows_by_file[file_name] = [row for row in all_rows if row["file_name"] == file_name]

    os.makedirs(os.path.dirname(args.out_md), exist_ok=True)
    os.makedirs(os.path.dirname(args.out_csv), exist_ok=True)

    write_markdown(rows_by_file, args.out_md)
    write_csv(all_rows_sorted, args.out_csv)

    timestamp = datetime.now().isoformat(timespec="seconds")
    print(f"Data dictionary written at {timestamp}")
    print(f"- {args.out_md}")
    print(f"- {args.out_csv}")


if __name__ == "__main__":
    main()
