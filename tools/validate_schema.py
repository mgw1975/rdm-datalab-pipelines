#!/usr/bin/env python3
import json
import os
import re
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Tuple


PROJECT_ID = "rdm-datalab-portfolio"
DATASET = "portfolio_data"
DDL_DIR = Path("bigquery/ddl")
REPORT_MD = Path("schema_diff_report.md")
REPORT_JSON = Path("schema_diff_summary.json")


@dataclass
class ColumnDef:
    name: str
    data_type: str
    mode: str  # NULLABLE or REQUIRED


def _strip_sql_comments(sql: str) -> str:
    # Remove -- comments and /* */ blocks.
    sql = re.sub(r"/\*.*?\*/", "", sql, flags=re.S)
    sql = re.sub(r"--.*?$", "", sql, flags=re.M)
    return sql


def _extract_table_name(sql: str) -> str:
    m = re.search(
        r"CREATE\s+(?:OR\s+REPLACE\s+)?TABLE\s+`([^`]+)`",
        sql,
        flags=re.I,
    )
    if not m:
        raise ValueError("Missing CREATE OR REPLACE TABLE statement.")
    full_name = m.group(1)
    parts = full_name.split(".")
    if len(parts) != 3:
        raise ValueError(f"Unexpected table identifier: {full_name}")
    project_id, dataset, table = parts
    if project_id != PROJECT_ID or dataset != DATASET:
        raise ValueError(
            f"DDL table {full_name} is outside {PROJECT_ID}.{DATASET} scope."
        )
    return table


def _extract_columns(sql: str) -> Dict[str, ColumnDef]:
    # Extract the column block between the first "(" after CREATE TABLE and the matching ");".
    # This is a simple parser that expects standard DDL formatting.
    ddl = _strip_sql_comments(sql)
    m = re.search(
        r"CREATE\s+(?:OR\s+REPLACE\s+)?TABLE\s+`[^`]+`\s*\((.*)\)\s*;",
        ddl,
        flags=re.I | re.S,
    )
    if not m:
        raise ValueError("Unable to locate column definitions block.")
    block = m.group(1)
    columns: Dict[str, ColumnDef] = {}
    for raw_line in block.splitlines():
        line = raw_line.strip().rstrip(",")
        if not line:
            continue
        if line.startswith(")"):
            continue
        # Skip constraints or table-level options if any.
        if re.match(r"^(PRIMARY|FOREIGN|CONSTRAINT|UNIQUE)\b", line, flags=re.I):
            continue
        parts = line.split()
        if len(parts) < 2:
            continue
        name = parts[0]
        data_type = parts[1].upper()
        mode = "REQUIRED" if re.search(r"\bNOT\s+NULL\b", line, flags=re.I) else "NULLABLE"
        columns[name] = ColumnDef(name=name, data_type=data_type, mode=mode)
    if not columns:
        raise ValueError("No columns parsed from DDL.")
    return columns


def _load_ddl_files() -> Dict[str, Dict[str, ColumnDef]]:
    ddl_map: Dict[str, Dict[str, ColumnDef]] = {}
    if not DDL_DIR.exists():
        raise FileNotFoundError(f"DDL directory not found: {DDL_DIR}")
    for path in sorted(DDL_DIR.glob("*.sql")):
        sql = path.read_text(encoding="utf-8")
        try:
            table = _extract_table_name(sql)
            ddl_map[table] = _extract_columns(sql)
        except Exception as exc:
            raise RuntimeError(f"Failed parsing DDL file: {path} ({exc})") from exc
    if not ddl_map:
        raise ValueError("No DDL files found under bigquery/ddl.")
    return ddl_map


def _bq_query(query: str) -> List[Dict[str, str]]:
    # Use bq CLI to avoid dependency on google-cloud-bigquery in repo.
    cmd = [
        "bq",
        "query",
        "--project_id",
        PROJECT_ID,
        "--use_legacy_sql=false",
        "--format=json",
        "--max_rows=100000",
        query,
    ]
    proc = subprocess.run(cmd, check=False, capture_output=True, text=True)
    if proc.returncode != 0:
        raise RuntimeError(
            "bq query failed:\n"
            f"STDOUT:\n{proc.stdout}\nSTDERR:\n{proc.stderr}"
        )
    return json.loads(proc.stdout or "[]")


def _fetch_bq_schema(tables: List[str]) -> Dict[str, Dict[str, ColumnDef]]:
    # Query INFORMATION_SCHEMA for all columns in the dataset, then filter in Python.
    query = f"""
    SELECT
      table_name,
      column_name,
      data_type,
      is_nullable
    FROM `{PROJECT_ID}.{DATASET}.INFORMATION_SCHEMA.COLUMNS`
    ORDER BY table_name, ordinal_position
    """
    rows = _bq_query(query)
    schema_map: Dict[str, Dict[str, ColumnDef]] = {}
    for row in rows:
        table = row["table_name"]
        if table not in tables:
            continue
        mode = "NULLABLE" if row["is_nullable"].upper() == "YES" else "REQUIRED"
        schema_map.setdefault(table, {})[row["column_name"]] = ColumnDef(
            name=row["column_name"],
            data_type=row["data_type"].upper(),
            mode=mode,
        )
    return schema_map


def _fetch_bq_tables() -> List[str]:
    query = f"""
    SELECT table_name
    FROM `{PROJECT_ID}.{DATASET}.INFORMATION_SCHEMA.TABLES`
    WHERE table_type = "BASE TABLE"
    ORDER BY table_name
    """
    rows = _bq_query(query)
    return [row["table_name"] for row in rows]


def _compare(
    ddl_cols: Dict[str, ColumnDef],
    bq_cols: Dict[str, ColumnDef],
) -> Dict[str, List[Tuple[str, str, str]]]:
    diffs = {
        "missing_in_bq": [],
        "extra_in_bq": [],
        "type_mismatches": [],
        "nullability_mismatches": [],
    }
    ddl_names = set(ddl_cols.keys())
    bq_names = set(bq_cols.keys())

    for name in sorted(ddl_names - bq_names):
        diffs["missing_in_bq"].append((name, ddl_cols[name].data_type, ddl_cols[name].mode))
    for name in sorted(bq_names - ddl_names):
        diffs["extra_in_bq"].append((name, bq_cols[name].data_type, bq_cols[name].mode))
    for name in sorted(ddl_names & bq_names):
        ddl_col = ddl_cols[name]
        bq_col = bq_cols[name]
        if ddl_col.data_type != bq_col.data_type:
            diffs["type_mismatches"].append(
                (name, ddl_col.data_type, bq_col.data_type)
            )
        if ddl_col.mode != bq_col.mode:
            diffs["nullability_mismatches"].append((name, ddl_col.mode, bq_col.mode))
    return diffs


def _render_markdown(
    per_table_diffs: Dict[str, Dict[str, List[Tuple[str, str, str]]]],
    missing_ddl_tables: List[str],
) -> str:
    lines: List[str] = ["# Schema Diff Report", ""]
    for table in sorted(per_table_diffs.keys()):
        diffs = per_table_diffs[table]
        lines.append(f"## `{DATASET}.{table}`")
        if all(len(v) == 0 for v in diffs.values()):
            lines.append("")
            lines.append("NO DIFFERENCES FOUND")
            lines.append("")
            continue
        if diffs["missing_in_bq"]:
            lines.append("")
            lines.append("### Missing in BigQuery")
            lines.append("")
            lines.append("| Column | DDL Type | DDL Mode |")
            lines.append("| --- | --- | --- |")
            for name, dtype, mode in diffs["missing_in_bq"]:
                lines.append(f"| {name} | {dtype} | {mode} |")
        if diffs["extra_in_bq"]:
            lines.append("")
            lines.append("### Extra in BigQuery")
            lines.append("")
            lines.append("| Column | BQ Type | BQ Mode |")
            lines.append("| --- | --- | --- |")
            for name, dtype, mode in diffs["extra_in_bq"]:
                lines.append(f"| {name} | {dtype} | {mode} |")
        if diffs["type_mismatches"]:
            lines.append("")
            lines.append("### Type Mismatches")
            lines.append("")
            lines.append("| Column | DDL Type | BQ Type |")
            lines.append("| --- | --- | --- |")
            for name, ddl_type, bq_type in diffs["type_mismatches"]:
                lines.append(f"| {name} | {ddl_type} | {bq_type} |")
        if diffs["nullability_mismatches"]:
            lines.append("")
            lines.append("### Nullability Mismatches")
            lines.append("")
            lines.append("| Column | DDL Mode | BQ Mode |")
            lines.append("| --- | --- | --- |")
            for name, ddl_mode, bq_mode in diffs["nullability_mismatches"]:
                lines.append(f"| {name} | {ddl_mode} | {bq_mode} |")
        lines.append("")
    if missing_ddl_tables:
        lines.append("## Tables Without DDL Files")
        lines.append("")
        lines.append(
            "The following BigQuery tables have no matching DDL file in `bigquery/ddl`:"
        )
        lines.append("")
        for name in missing_ddl_tables:
            lines.append(f"- `{DATASET}.{name}`")
        lines.append("")
    return "\n".join(lines).strip() + "\n"


def _summarize_json(
    per_table_diffs: Dict[str, Dict[str, List[Tuple[str, str, str]]]]
) -> Dict[str, Dict[str, int]]:
    summary: Dict[str, Dict[str, int]] = {}
    for table, diffs in per_table_diffs.items():
        status = "MATCH" if all(len(v) == 0 for v in diffs.values()) else "MISMATCH"
        summary[table] = {
            "status": status,
            "missing_in_bq": len(diffs["missing_in_bq"]),
            "extra_in_bq": len(diffs["extra_in_bq"]),
            "type_mismatches": len(diffs["type_mismatches"]),
            "nullability_mismatches": len(diffs["nullability_mismatches"]),
        }
    return summary


def main() -> int:
    ddl_map = _load_ddl_files()
    ddl_tables = set(ddl_map.keys())

    bq_tables = _fetch_bq_tables()
    # Only compare tables that exist in BigQuery and have a DDL file in repo.
    tables = sorted(t for t in bq_tables if t in ddl_tables)
    missing_ddl_tables = sorted(t for t in bq_tables if t not in ddl_tables)
    bq_map = _fetch_bq_schema(tables)
    missing_schema = [t for t in tables if t not in bq_map or not bq_map[t]]
    if missing_schema:
        raise RuntimeError(
            "BigQuery tables missing schema details: " + ", ".join(missing_schema)
        )

    per_table_diffs: Dict[str, Dict[str, List[Tuple[str, str, str]]]] = {}
    for table in tables:
        per_table_diffs[table] = _compare(ddl_map[table], bq_map[table])

    REPORT_MD.write_text(
        _render_markdown(per_table_diffs, missing_ddl_tables), encoding="utf-8"
    )
    REPORT_JSON.write_text(
        json.dumps(_summarize_json(per_table_diffs), indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )

    print(f"Compared tables: {len(tables)}")
    for table in tables:
        diffs = per_table_diffs[table]
        status = "MATCH" if all(len(v) == 0 for v in diffs.values()) else "MISMATCH"
        print(
            f"{DATASET}.{table}: {status} "
            f"(missing_in_bq={len(diffs['missing_in_bq'])}, "
            f"extra_in_bq={len(diffs['extra_in_bq'])}, "
            f"type_mismatches={len(diffs['type_mismatches'])}, "
            f"nullability_mismatches={len(diffs['nullability_mismatches'])})"
        )
    if missing_ddl_tables:
        print(f"BigQuery tables without DDL: {len(missing_ddl_tables)}")

    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        sys.exit(1)
