# RDM Datalab Pipelines

A tidy home for your ABS and QCEW pipelines. This repo follows a lightweight, reproducible layout.

## Layout
```
rdm-datalab-pipelines/
├── scripts/                    # CLI pipelines
│   ├── rdm_abs_naics3_cbsa.py
│   ├── qcew_prep_naics2.py
│   └── qcew_prep_naics_sector.py
├── notebooks/                  # step-through analysis notebooks
│   ├── ABS_NAICS3_CBSA_stepthrough.ipynb
│   ├── QCEW_NAICS2_stepthrough.ipynb
│   └── QCEW_NAICS_sector_stepthrough.ipynb
├── data/
│   ├── raw/                    # source downloads (not committed; use .gitignore)
│   ├── interim/                # intermediate artifacts
│   └── processed/              # cleaned outputs ready for analysis
├── outputs/                    # figures/tables for sharing (optional)
├── metadata/                   # config, manifests, and QA reports
├── Makefile                    # handy commands
├── environment.yml             # reproducible environment
├── .gitignore
└── README.md
```

## Quick start
```bash
# create conda/mamba env
mamba env create -f environment.yml || conda env create -f environment.yml
conda activate rdm-datalab

# run QCEW sector prep (example)
python scripts/qcew_prep_naics_sector.py   --qcew_raw data/raw/2022_annual_singlefile.csv   --year 2022   --out data/processed/qcew_county_naics_sector_2022.csv

# run ABS → CBSA
python scripts/rdm_abs_naics3_cbsa.py   --abs data/raw/abs_county_naics3.csv   --xwalk data/raw/cbsa_county_crosswalk.csv   --year 2022   --large_by firms --large_threshold 20000   --outdir data/processed
```

## Versioning & 'latest'
- Keep code versioned via **git** with semantic tags (e.g., `v0.2.0`).
- Write outputs to `data/processed/YYYYMMDD/…` and also copy to `data/processed/LATEST/` for convenience.
- Store QA reports in `metadata/qa_*.csv` so you can track suppression & coverage over time.

## Conventions
- All FIPS are zero-padded strings (`state_fips=2`, `county_fips=3`).
- **QCEW wages** already in dollars. **ABS payroll/receipts** converted to dollars.
- Sector-level QCEW uses `agglvl_code=74` and `own_code=0` (falls back to `5` if needed).
