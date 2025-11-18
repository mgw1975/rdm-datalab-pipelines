#!/usr/bin/env python3
"""
Create a compact, spreadsheet-friendly summary from dictionary_output.json.

Usage:
  python make_summary.py [--in dictionary_output.json] [--out dictionary_summary.csv]
"""

import argparse, json, os, sys
import pandas as pd

def main():
    p = argparse.ArgumentParser()
    p.add_argument("--in", dest="in_path", default="dictionary_output.json",
                   help="Path to the JSON returned by the API (default: dictionary_output.json)")
    p.add_argument("--out", dest="out_path", default="dictionary_summary.csv",
                   help="Path for the CSV summary to write (default: dictionary_summary.csv)")
    args = p.parse_args()

    if not os.path.exists(args.in_path):
        sys.stderr.write(f"[ERROR] Input JSON not found: {args.in_path}\n"
                         f"Tip: re-run curl with: -o {args.in_path}\n")
        sys.exit(1)

    # Load JSON safely
    try:
        with open(args.in_path, "r", encoding="utf-8") as f:
            d = json.load(f)
    except json.JSONDecodeError as e:
        sys.stderr.write(f"[ERROR] Could not parse JSON in {args.in_path}: {e}\n"
                         f"Tip: Open the file and check whether the API returned an HTML error or partial output.\n")
        sys.exit(1)

    # Validate shape
    if not isinstance(d, dict):
        sys.stderr.write("[ERROR] Unexpected JSON structure (top-level is not an object)\n")
        sys.exit(1)

    fields = d.get("fields")
    if not isinstance(fields, list):
        sys.stderr.write("[ERROR] JSON missing `fields` array. Top-level keys present: "
                         f"{list(d.keys())}\n"
                         "Tip: The service may have returned an error; check for `detail` or `message` keys.\n")
        sys.exit(1)

    # Build rows defensively
    rows = []
    for f in fields:
        stats = f.get("stats", {}) or {}
        row = {
            "name": f.get("name", ""),
            "definition": f.get("definition", ""),
            "autofill_source": f.get("autofill_source", ""),
            "autofill_confidence": f.get("autofill_confidence", ""),
            "declared_type": f.get("declared_type", ""),
            "observed_type": f.get("observed_type", ""),
            "distinct": stats.get("distinct_count", ""),
            "missing_pct": stats.get("missing_pct", ""),
            "min": stats.get("min", ""),
            "p50": stats.get("p50", ""),
            "max": stats.get("max", ""),
            "examples": ", ".join(map(str, (f.get("examples") or [])[:3])),
            "notes": f.get("notes", "")
        }
        rows.append(row)

    df = pd.DataFrame(rows)
    df.to_csv(args.out_path, index=False)

    # Print a tiny report to the terminal
    ds = d.get("dataset_summary", {})
    print("✅ Wrote:", args.out_path)
    print("Dataset summary:",
          f"rows={ds.get('rows','?')}, cols={ds.get('cols','?')}, memMB={ds.get('memory_mb','?')}")
    print("Fields:", len(df))
    if d.get("warnings"):
        print("Warnings:")
        for w in d["warnings"]:
            print("  -", w)

if __name__ == "__main__":
    main()

