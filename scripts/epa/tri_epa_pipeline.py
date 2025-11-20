#!/usr/bin/env python3
"""
EPA TRI 1A county × NAICS2 aggregation with county-fips enrichment.

This mirrors the logic in notebooks/epa/tri_epa.ipynb so the pipeline can be
run non-interactively:
  1. Read the EPA 1A tab-delimited release file (robust to ragged rows).
  2. Derive total release pounds (on-site + off-site) per facility.
  3. Aggregate to (state, county name, NAICS2) totals.
  4. Enrich with Simplemaps county reference to attach 5-digit FIPS codes.
"""

from __future__ import annotations

import argparse
import csv
import re
from pathlib import Path
from typing import Iterable, List, Optional

import pandas as pd

DEFAULT_TRI_PATH = Path("data_raw/us_series/US_1a_2022.txt")
DEFAULT_SIMPLEMAPS = Path(
    "data_raw/external/simplemaps/simplemaps_uscounties_basicv1.91/uscounties.csv"
)
DEFAULT_OUT = Path("data_clean/tri/tri_epa.csv")

COUNTY_SUFFIX_TERMS = [
    "county",
    "parish",
    "borough",
    "boro",
    "municipio",
    "municipality",
    "census area",
    "census are",
    "censu",
    "census district",
    "city and borough",
    "city",
    "island",
]
TERRITORY_SKIP = {"PR", "VI"}


def normalize_row(fields: List[str], width: int) -> List[str]:
    """Return exactly `width` fields by padding or gluing overflow."""
    if len(fields) < width:
        return fields + [""] * (width - len(fields))
    if len(fields) > width:
        glued = fields[: width - 1] + ["\t".join(fields[width - 1 :])]
        return glued
    return fields


def read_tri_1a(tri_path: Path) -> pd.DataFrame:
    """Read the EPA TRI 1A TSV export (with no header row)."""
    tri_path = Path(tri_path)
    if not tri_path.exists():
        raise FileNotFoundError(tri_path)

    header_cols: Optional[List[str]] = None
    rows: List[List[str]] = []
    with tri_path.open("r", encoding="latin1", newline="") as fh:
        for raw_line in fh:
            line = raw_line.lstrip("\ufeff").rstrip("\r\n")
            if not line or line.lower().startswith("total output lines"):
                continue
            header_cols = line.split("\t")
            if header_cols and (header_cols[-1] == "" or header_cols[-1].isdigit()):
                header_cols = header_cols[:-1]
            width = len(header_cols)
            reader = csv.reader(
                fh, delimiter="\t", quotechar='"', doublequote=True, escapechar="\\"
            )
            for record in reader:
                if not record:
                    continue
                if record[0].lower().startswith("total output lines"):
                    continue
                rows.append(normalize_row(record, width))
            break

    if header_cols is None:
        raise ValueError(f"Could not locate header row in {tri_path}")

    df = pd.DataFrame(rows, columns=header_cols)
    df = df.loc[:, ~df.columns.astype(str).str.fullmatch(r"Unnamed:.*|^$")]
    return df


def find_column(columns: Iterable[str], keyword: str) -> Optional[str]:
    keyword = keyword.upper()
    for col in columns:
        if keyword in col.upper():
            return col
    return None


def derive_tri_aggregates(df: pd.DataFrame) -> pd.DataFrame:
    naics_col = find_column(df.columns, "PRIMARY NAICS CODE")
    state_col = find_column(df.columns, "FACILITY STATE")
    county_col = find_column(df.columns, "FACILITY COUNTY")
    col_on = find_column(df.columns, "TOTAL ON-SITE RELEASES")
    col_off = find_column(df.columns, "TOTAL TRANSFERRED OFF SITE FOR DISPOSAL")

    required = {
        "primary NAICS": naics_col,
        "facility state": state_col,
        "facility county": county_col,
        "total on-site releases": col_on,
        "total off-site releases": col_off,
    }
    missing = [name for name, col in required.items() if col is None]
    if missing:
        raise ValueError(f"TRI file missing required columns: {missing}")

    df = df.copy()
    df["naics2_sector_cd"] = (
        df[naics_col].astype(str).str.extract(r"(\d+)", expand=False).str[:2]
    )
    df["state_cd"] = (
        df[state_col].astype(str).str.strip().str.upper().str[:2]
    )  # e.g., CA, NY
    df["cnty_nm"] = df[county_col].astype(str).str.strip().str.upper()

    df["tri_ttl_rls_lbs_amt"] = (
        pd.to_numeric(df[col_on], errors="coerce").fillna(0)
        + pd.to_numeric(df[col_off], errors="coerce").fillna(0)
    )

    tri_g = (
        df.dropna(
            subset=["state_cd", "cnty_nm", "naics2_sector_cd", "tri_ttl_rls_lbs_amt"]
        )
        .groupby(["state_cd", "cnty_nm", "naics2_sector_cd"], as_index=False)[
            "tri_ttl_rls_lbs_amt"
        ]
        .sum()
    )
    return tri_g


def normalize_county_name(series: pd.Series) -> pd.Series:
    cleaned = series.fillna("").str.lower().str.replace(r"[^a-z0-9\s]", " ", regex=True)
    for term in COUNTY_SUFFIX_TERMS:
        pattern = r"\b" + term.replace(" ", r"\s+") + r"\b"
        cleaned = cleaned.str.replace(pattern, " ", regex=True)
    cleaned = (
        cleaned.str.replace(r"\bst\b", "saint", regex=True)
        .str.replace(r"\band\b", "and", regex=True)
        .str.replace(r"\s+", "", regex=True)
        .str.strip()
    )
    return cleaned


def build_county_lookup(simplemaps_csv: Path) -> pd.DataFrame:
    county_ref = pd.read_csv(simplemaps_csv, dtype=str)
    name_cols = ["county", "county_ascii", "county_full"]
    for col in name_cols:
        county_ref[f"{col}_norm"] = normalize_county_name(county_ref[col])

    lookup = (
        county_ref.assign(state_code=county_ref["state_id"].str.upper())
        .melt(
            id_vars=["state_code", "county_fips"],
            value_vars=[f"{col}_norm" for col in name_cols],
            value_name="county_name_norm",
        )
        .dropna(subset=["county_name_norm"])
        .drop_duplicates(["state_code", "county_name_norm"])
        .assign(county_fips_5=lambda df: df["county_fips"].astype(str).str.zfill(5))
    )

    manual_overrides = pd.DataFrame(
        [
            ("CT", "fairfield", "09001"),
            ("CT", "hartford", "09003"),
            ("CT", "litchfield", "09005"),
            ("CT", "middlesex", "09007"),
            ("CT", "newhaven", "09009"),
            ("CT", "newlondon", "09011"),
            ("CT", "tolland", "09013"),
            ("CT", "windham", "09015"),
            ("AK", "valdezcordova", "02261"),
        ],
        columns=["state_code", "county_name_norm", "county_fips_5"],
    )
    lookup = (
        pd.concat([lookup, manual_overrides], ignore_index=True)
        .drop_duplicates(["state_code", "county_name_norm"], keep="last")
        .reset_index(drop=True)
    )
    return lookup


def enrich_with_fips(tri_g: pd.DataFrame, lookup: pd.DataFrame) -> pd.DataFrame:
    tri_normed = tri_g.assign(
        state_code=tri_g["state_cd"].str.upper(),
        county_name_norm=normalize_county_name(tri_g["cnty_nm"]),
    )
    merged = tri_normed.merge(
        lookup, on=["state_code", "county_name_norm"], how="left"
    )
    merged["state_cnty_fips_cd"] = merged["county_fips_5"]
    merged["state_fips_cd"] = merged["county_fips_5"].str[:2]
    merged["county_fips_cd"] = merged["county_fips_5"].str[2:]

    resolvable_mask = ~merged["state_code"].isin(TERRITORY_SKIP)
    resolved = merged["state_cnty_fips_cd"].notna() & resolvable_mask
    if resolvable_mask.any():
        pct = resolved.sum() / resolvable_mask.sum()
        print(
            f"Resolved {resolved.sum():,} of {resolvable_mask.sum():,} "
            f"resolvable facilities ({pct:.2%})."
        )
    remaining = merged.loc[
        resolvable_mask & merged["state_cnty_fips_cd"].isna(), ["state_cd", "cnty_nm"]
    ].drop_duplicates()
    if not remaining.empty:
        print(
            "Remaining non-PR/VI mismatches (check GU/MP/AS coverage in Simplemaps):"
        )
        print(remaining.head(10).to_string(index=False))

    assert merged["state_cnty_fips_cd"].dropna().str.len().eq(5).all()
    assert merged["county_fips_cd"].dropna().str.len().eq(3).all()
    tri_final = merged[
        ["state_cd", "cnty_nm", "state_cnty_fips_cd", "naics2_sector_cd", "tri_ttl_rls_lbs_amt"]
    ].copy()
    tri_final["tri_ttl_rls_lbs_amt"] = (
        pd.to_numeric(tri_final["tri_ttl_rls_lbs_amt"], errors="coerce")
        .fillna(0)
        .round(2)
    )
    return tri_final


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Aggregate EPA TRI 1A file to county × NAICS2 with FIPS enrichment."
    )
    parser.add_argument(
        "--tri_txt",
        default=str(DEFAULT_TRI_PATH),
        help=f"Raw EPA 1A TSV export (default: {DEFAULT_TRI_PATH})",
    )
    parser.add_argument(
        "--simplemaps",
        default=str(DEFAULT_SIMPLEMAPS),
        help=f"Simplemaps uscounties CSV (default: {DEFAULT_SIMPLEMAPS})",
    )
    parser.add_argument(
        "--out_csv",
        default=str(DEFAULT_OUT),
        help=f"Output CSV path (default: {DEFAULT_OUT})",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    tri_raw = read_tri_1a(Path(args.tri_txt))
    tri_g = derive_tri_aggregates(tri_raw)
    lookup = build_county_lookup(Path(args.simplemaps))
    tri_final = enrich_with_fips(tri_g, lookup)

    out_path = Path(args.out_csv)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    tri_final.to_csv(out_path, index=False)
    print(f"Wrote {out_path} with {len(tri_final):,} rows.")


if __name__ == "__main__":
    main()
