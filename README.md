# RDM Datalab Pipelines

A tidy home for your ABS and QCEW pipelines. This repo follows a lightweight, reproducible layout.

## Layout
```
rdm-datalab-pipelines/
├── data_raw/                   # untouched downloads (ABS, QCEW, BEA, EPA, crosswalks)
├── data_clean/                 # production-ready facts and reference tables
├── bigquery/
│   ├── ddl/                    # warehouse table definitions
│   ├── dml/                    # load/merge SQL
│   └── views/                  # analytical view definitions
├── notebooks/
│   ├── abs/                    # ABS exploration + step-throughs
│   ├── qcew/
│   ├── bea/
│   ├── epa/
│   ├── integration/
│   └── reference/
├── scripts/
│   ├── abs/                    # ABS CLI utilities
│   ├── qcew/                   # QCEW prep utilities
│   ├── bea/                    # BEA helpers (planned)
│   └── epa/                    # EPA TRI/GHGRP helpers (planned)
├── services/                   # FastAPI and other service surfaces
│   └── data_dictionary/
├── metadata/                   # config, manifests, QA reports
├── outputs/                    # shareable figures/tables (optional)
├── docs/                       # glossary, conventions, design notes
├── misc/                       # temporary or redundant staging area
├── Makefile
├── environment.yml
├── .gitignore
└── README.md
```

## Quick start
```bash
# create conda/mamba env
mamba env create -f environment.yml || conda env create -f environment.yml
conda activate rdm-datalab

# run QCEW sector prep (example)
python scripts/qcew/qcew_prep_naics_sector.py \
  --qcew_raw data_raw/2022_annual_singlefile.csv \
  --year 2022 \
  --out data_clean/qcew_county_naics_sector_2022.csv

# run ABS → CBSA
python scripts/abs/rdm_abs_naics3_cbsa.py \
  --abs data_raw/abs_county_naics3.csv \
  --xwalk data_raw/cbsa_county_crosswalk.csv \
  --year 2022 \
  --large_by firms --large_threshold 20000 \
  --outdir data_clean
```

## Versioning & 'latest'
- Keep code versioned via **git** with semantic tags (e.g., `v0.2.0`).
- Write outputs to `data_clean/YYYYMMDD/…` and also copy to `data_clean/LATEST/` for convenience.
- Store QA reports in `metadata/qa_*.csv` so you can track suppression & coverage over time.

## Conventions
- All FIPS are zero-padded strings (`state_fips=2`, `county_fips=3`).
- **QCEW wages** already in dollars. **ABS payroll/receipts** converted to dollars.
- Sector-level QCEW uses `agglvl_code=74` and `own_code=0` (falls back to `5` if needed).
