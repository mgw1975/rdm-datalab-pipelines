# Load prepared NAICS2 reference CSV into BigQuery.
# 1. Generate the CSV via `python scripts/refs/prep_ref_naics2.py`
# 2. Upload it to GCS (example path below), then run:

bq load \
  --replace=true \
  --source_format=CSV \
  --skip_leading_rows=1 \
  rdm-datalab-portfolio:portfolio_data.ref_naics2_uscb \
  gs://rdm_datalab_portfolio/reference/ref_naics2_uscb.csv \
  naics2_sector_cd:STRING,naics2_sector_desc:STRING
