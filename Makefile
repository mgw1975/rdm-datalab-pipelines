SHELL := /bin/bash

ENV ?= rdm-datalab
YEAR ?= 2022

.PHONY: env qcew_sector abs_cbsa

env:
	mamba env create -f environment.yml || conda env create -f environment.yml || true
	@echo "Run: conda activate $(ENV)"

qcew_sector:
	python scripts/qcew_prep_naics_sector.py --qcew_raw data/raw/2022_annual_singlefile.csv --year $(YEAR) --out data/processed/qcew_county_naics_sector_$(YEAR).csv

abs_cbsa:
	python scripts/rdm_abs_naics3_cbsa.py --abs data/raw/abs_county_naics3.csv --xwalk data/raw/cbsa_county_crosswalk.csv --year $(YEAR) --large_by firms --large_threshold 20000 --outdir data/processed
