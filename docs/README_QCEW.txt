
QCEW 2022 — county × NAICS2 prep (README)
=========================================

Overview
--------
This module prepares **BLS QCEW** wages at the target grain:
**county × 2‑digit NAICS × year** with fields:
- `annual_avg_emplvl`
- `total_annual_wages`
- `avg_weekly_wage`

Outputs are in **dollars** (QCEW is already in dollars). When merging with ABS,
remember to convert ABS payroll/receipts from $1,000s → dollars.

Inputs
------
A raw QCEW annual CSV (2022) that includes columns (synonyms handled):
- `area_fips` (5‑digit county FIPS)
- `industry_code` (NAICS)
- `year`
- `annual_avg_emplvl`
- `total_annual_wages`
- `avg_wkly_wage` (or `avg_weekly_wage`)
- Optional: `own_code` (we keep `own_code == "0"` if present — “Total covered”)

Output schema
-------------
`qcew_county_naics2_2022.csv` with columns:
- `state_fips` (2)
- `county_fips` (3)
- `naics2` (2‑digit NAICS)
- `year`
- `annual_avg_emplvl`
- `total_annual_wages`
- `avg_weekly_wage`

Usage
-----
**CLI**
```
python qcew_prep_naics2.py   --qcew_raw qcew_annual_raw_2022.csv   --year 2022   --out qcew_county_naics2_2022.csv
```

**Notebook**
Open `QCEW_NAICS2_stepthrough.ipynb`, edit the Parameters cell:
```
QCEW_RAW = "qcew_annual_raw_2022.csv"
YEAR = 2022
OUT = "qcew_county_naics2_2022.csv"
```
Run cells top‑to‑bottom.

Data handling notes
-------------------
- Keeps only **county‑level** rows: `len(area_fips) == 5`.
- Derives `naics2` from the **first 2 digits** of `industry_code`. Only 2‑digit rows are kept.
- Filters to `own_code == "0"` if `own_code` exists (Total covered).
- Collapses duplicate rows to one record per (`state_fips`, `county_fips`, `naics2`, `year`):
  - `annual_avg_emplvl`: sum
  - `total_annual_wages`: sum
  - `avg_weekly_wage`: recomputed as `total_annual_wages / (annual_avg_emplvl * 52)` when possible.

Merging with ABS (later step)
-----------------------------
- Convert ABS `PAYANN` and `RCPPDEMP` from **$1,000s → dollars** before comparison/ratios.
- Join keys: `state_fips`, `county_fips`, `naics2`, `year`.
- Prefer **ratios** for cross‑source benchmarking:
  - `qcew_wage_per_emp = total_annual_wages / annual_avg_emplvl`
  - `abs_wage_per_emp = abs_payroll_usd / abs_emp`
  - `abs_receipts_per_firm = abs_receipts_usd / abs_firms`

Quick QA checks
---------------
- Unique key: no duplicates for (`state_fips`, `county_fips`, `naics2`, `year`).
- Non‑negative numerics.
- Spot‑check a few large counties (e.g., LA County, Cook, Harris) against source values.

Troubleshooting
---------------
- **Missing required columns**: Your raw file uses different headers — the script tries common synonyms; rename if needed.
- **Too few rows**: Ensure you supplied the **annual** file and that `area_fips` includes **counties** (5 digits).
- **Avg weekly wage looks off**: It’s recomputed from totals; verify employment and wage units are dollars.

Files
-----
- CLI script: `qcew_prep_naics2.py`
- Notebook: `QCEW_NAICS2_stepthrough.ipynb`
- This README: `README_QCEW.txt`

Version
-------
2025‑09‑15
