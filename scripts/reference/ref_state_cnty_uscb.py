#!/usr/bin/env python3
"""
Convert the 2022 USCB Gazetteer county file into the ref_state_cnty_uscb CSV.

Example:
    python scripts/reference/ref_state_cnty_uscb.py \
        --src data_raw/reference/2022_Gaz_counties_national.txt \
        --out data_clean/reference/ref_state_cnty_uscb.csv
"""

import argparse
import math
from pathlib import Path
from typing import List

import pandas as pd

COLUMN_MAP = {
    "GEOID": "state_cnty_fips_cd",
    "USPS": "state_cd",
    "ANSICODE": "cnty_ansi_nm",
    "NAME": "cnty_nm",
    "ALAND": "land_area_num",
    "AWATER": "water_area_num",
    "INTPTLAT": "lat_num",
    "INTPTLONG": "long_num",
}
OUTPUT_COLUMNS = [
    "state_cnty_fips_cd",
    "state_cd",
    "cnty_ansi_nm",
    "cnty_nm",
    "land_area_num",
    "water_area_num",
    "lat_num",
    "long_num",
]
INT_COLUMNS = ["land_area_num", "water_area_num"]
FLOAT_COLUMNS = ["lat_num", "long_num"]
TEXT_COLUMNS = ["state_cnty_fips_cd", "state_cd", "cnty_ansi_nm", "cnty_nm"]

STATE_METADATA = [
    ("01", "AL", "Alabama"),
    ("02", "AK", "Alaska"),
    ("04", "AZ", "Arizona"),
    ("05", "AR", "Arkansas"),
    ("06", "CA", "California"),
    ("08", "CO", "Colorado"),
    ("09", "CT", "Connecticut"),
    ("10", "DE", "Delaware"),
    ("11", "DC", "District of Columbia"),
    ("12", "FL", "Florida"),
    ("13", "GA", "Georgia"),
    ("15", "HI", "Hawaii"),
    ("16", "ID", "Idaho"),
    ("17", "IL", "Illinois"),
    ("18", "IN", "Indiana"),
    ("19", "IA", "Iowa"),
    ("20", "KS", "Kansas"),
    ("21", "KY", "Kentucky"),
    ("22", "LA", "Louisiana"),
    ("23", "ME", "Maine"),
    ("24", "MD", "Maryland"),
    ("25", "MA", "Massachusetts"),
    ("26", "MI", "Michigan"),
    ("27", "MN", "Minnesota"),
    ("28", "MS", "Mississippi"),
    ("29", "MO", "Missouri"),
    ("30", "MT", "Montana"),
    ("31", "NE", "Nebraska"),
    ("32", "NV", "Nevada"),
    ("33", "NH", "New Hampshire"),
    ("34", "NJ", "New Jersey"),
    ("35", "NM", "New Mexico"),
    ("36", "NY", "New York"),
    ("37", "NC", "North Carolina"),
    ("38", "ND", "North Dakota"),
    ("39", "OH", "Ohio"),
    ("40", "OK", "Oklahoma"),
    ("41", "OR", "Oregon"),
    ("42", "PA", "Pennsylvania"),
    ("44", "RI", "Rhode Island"),
    ("45", "SC", "South Carolina"),
    ("46", "SD", "South Dakota"),
    ("47", "TN", "Tennessee"),
    ("48", "TX", "Texas"),
    ("49", "UT", "Utah"),
    ("50", "VT", "Vermont"),
    ("51", "VA", "Virginia"),
    ("53", "WA", "Washington"),
    ("54", "WV", "West Virginia"),
    ("55", "WI", "Wisconsin"),
    ("56", "WY", "Wyoming"),
    ("72", "PR", "Puerto Rico"),
    ("78", "VI", "U.S. Virgin Islands"),
]

MANUAL_COUNTY_SUPPLEMENTS = [
    ("09001", "CT", "Fairfield County"),
    ("09003", "CT", "Hartford County"),
    ("09005", "CT", "Litchfield County"),
    ("09007", "CT", "Middlesex County"),
    ("09009", "CT", "New Haven County"),
    ("09011", "CT", "New London County"),
    ("09013", "CT", "Tolland County"),
    ("09015", "CT", "Windham County"),
    ("78010", "VI", "St. Croix Island"),
    ("78020", "VI", "St. John Island"),
    ("78030", "VI", "St. Thomas Island"),
]


def load_gazetteer(src_path: Path) -> pd.DataFrame:
    """Load the Gazetteer tab-delimited file."""
    df = pd.read_csv(src_path, sep="\t", dtype=str, keep_default_na=False)
    df.columns = [col.strip() for col in df.columns]
    return df


def ensure_columns(df: pd.DataFrame, required: List[str]) -> None:
    """Raise if any required column is missing."""
    missing = sorted(set(required) - set(df.columns))
    if missing:
        raise ValueError(f"Input file missing expected columns: {missing}")


def tidy_gazetteer(df: pd.DataFrame) -> pd.DataFrame:
    """Rename, filter, and coerce the Gazetteer columns for BigQuery."""
    ensure_columns(df, list(COLUMN_MAP.keys()))
    tidy = df.rename(columns=COLUMN_MAP, errors="ignore")
    tidy = tidy[OUTPUT_COLUMNS].copy()

    for col in TEXT_COLUMNS:
        tidy[col] = tidy[col].astype(str).str.strip()

    tidy["state_cnty_fips_cd"] = tidy["state_cnty_fips_cd"].str.zfill(5)
    tidy = tidy[tidy["state_cnty_fips_cd"].str.match(r"^\d{5}$")]

    for col in INT_COLUMNS:
        tidy[col] = pd.to_numeric(tidy[col], errors="coerce").astype("Int64")
    for col in FLOAT_COLUMNS:
        tidy[col] = pd.to_numeric(tidy[col], errors="coerce")

    tidy = tidy.drop_duplicates(subset=["state_cnty_fips_cd"])
    tidy = append_supplemental_rows(tidy)
    tidy = tidy.sort_values("state_cnty_fips_cd").reset_index(drop=True)
    return tidy


def make_blank_row(state_cnty_fips_cd: str, state_cd: str, cnty_nm: str) -> dict:
    """Create a minimal reference row with placeholder numeric values."""
    return {
        "state_cnty_fips_cd": state_cnty_fips_cd,
        "state_cd": state_cd,
        "cnty_ansi_nm": pd.NA,
        "cnty_nm": cnty_nm,
        "land_area_num": pd.NA,
        "water_area_num": pd.NA,
        "lat_num": math.nan,
        "long_num": math.nan,
    }


def build_state_rollups() -> pd.DataFrame:
    """Create statewide and unspecified county rows for each state/territory."""
    rows = []
    for state_fips, state_cd, state_nm in STATE_METADATA:
        rows.append(
            make_blank_row(
                f"{state_fips}000", state_cd, f"{state_nm} (statewide aggregation)"
            )
        )
        rows.append(
            make_blank_row(
                f"{state_fips}999", state_cd, f"{state_nm} (unspecified county)"
            )
        )
    return pd.DataFrame(rows)


def build_manual_supplements() -> pd.DataFrame:
    """Rows for geographies no longer shipped in the Gazetteer (e.g., CT counties)."""
    rows = [
        make_blank_row(state_cnty_fips_cd, state_cd, cnty_nm)
        for state_cnty_fips_cd, state_cd, cnty_nm in MANUAL_COUNTY_SUPPLEMENTS
    ]
    return pd.DataFrame(rows)


def append_supplemental_rows(df: pd.DataFrame) -> pd.DataFrame:
    """Append rollup and manual geographies that are absent from the Gazetteer."""
    supplements = pd.concat(
        [build_state_rollups(), build_manual_supplements()], ignore_index=True
    )
    supplements = supplements[
        ~supplements["state_cnty_fips_cd"].isin(df["state_cnty_fips_cd"])
    ]
    if supplements.empty:
        return df
    supplements = supplements.reindex(columns=df.columns)
    supplements = supplements.astype(df.dtypes.to_dict())
    combined = pd.concat([df, supplements], ignore_index=True)
    return combined


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Prepare ref_state_cnty_uscb CSV from Gazetteer counties."
    )
    parser.add_argument(
        "--src",
        required=True,
        help="Path to 2022_Gaz_counties_national.txt",
    )
    parser.add_argument(
        "--out",
        required=True,
        help="Destination CSV path (e.g. data_clean/reference/ref_state_cnty_uscb.csv)",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    src_path = Path(args.src)
    out_path = Path(args.out)

    df_raw = load_gazetteer(src_path)
    tidy = tidy_gazetteer(df_raw)

    out_path.parent.mkdir(parents=True, exist_ok=True)
    tidy.to_csv(out_path, index=False)
    print(f"Wrote {out_path} with {len(tidy):,} counties.")


if __name__ == "__main__":
    main()
