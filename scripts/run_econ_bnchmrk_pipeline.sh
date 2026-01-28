#!/usr/bin/env bash
#
# Master runner for the econ benchmarking pipeline (ABS + QCEW).
# Steps:
#   1. Build private-only QCEW county × NAICS2 outputs for the selected years.
#   2. Download + prep employer-only ABS extracts for the same years.
#   3. Merge ABS + QCEW, derive shared metrics, and write the integration CSV.
#   4. Execute QA checks (structure + county coverage).
#   5. Placeholder: upload artifacts to GCS + load BigQuery tables once GCP
#      authentication is restored.
#
# Usage:
#   ./scripts/run_econ_bnchmrk_pipeline.sh          # uses defaults (2022-2023)
#   YEARS="2022 2023 2024" ./scripts/run_econ_bnchmrk_pipeline.sh
#

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT_DIR}"

YEARS="${YEARS:-"2022 2023"}"
ABS_OUT="${ABS_OUT:-data_clean/abs/econ_bnchmrk_abs_multiyear.csv}"
QCEW_OUT="${QCEW_OUT:-data_clean/qcew/econ_bnchmrk_qcew_multiyear.csv}"
MERGED_OUT="${MERGED_OUT:-data_clean/integration/econ_bnchmrk_abs_qcew.csv}"

read -r -a YEAR_ARR <<< "${YEARS}"

log() {
  echo "[PIPELINE] $*"
}

run_qcew() {
  log "Running QCEW prep for years: ${YEARS}"
  python scripts/qcew/econ_bnchmrk_qcew.py \
    --years "${YEAR_ARR[@]}" \
    --out "${QCEW_OUT}"
}

run_abs() {
  log "Running ABS prep for years: ${YEARS}"
  python scripts/abs/econ_bnchmrk_abs.py \
    --years "${YEAR_ARR[@]}" \
    --out_csv "${ABS_OUT}"
}

#run_merge() {
#  log "Merging ABS + QCEW extracts"
#  python scripts/integration/econ_bnchmrk_abs_qcew_merge.py \
#    --years "${YEAR_ARR[@]}" \
#    --out "${MERGED_OUT}"
#}

run_qa() {
  log "Executing QA checks"
  python qa/econ_bnchmrk_abs_qcew_qa.py
}

run_qcew
run_abs
#un_merge
run_qa

log "Pipeline artifacts ready:"
log "  ABS  → ${ABS_OUT}"
log "  QCEW → ${QCEW_OUT}"
log "  ABS+QCEW merged → ${MERGED_OUT}"

cat <<'TODO'
[PIPELINE] TODO: Configure GCP authentication + data loads
  - gcloud auth application-default login
  - gcloud auth login
  - Once auth is set up, run:
      gsutil cp data_clean/qcew/econ_bnchmrk_qcew_multiyear.csv gs://<bucket>/econ_bnchmrk_qcew_multiyear.csv
      gsutil cp data_clean/abs/econ_bnchmrk_abs_multiyear.csv gs://<bucket>/econ_bnchmrk_abs_multiyear.csv
      bq load --source_format=CSV ... rdm-datalab-portfolio:portfolio_data.econ_bnchmrk_qcew gs://<bucket>/econ_bnchmrk_qcew_multiyear.csv <schema>
      bq load --source_format=CSV ... rdm-datalab-portfolio:portfolio_data.econ_bnchmrk_abs gs://<bucket>/econ_bnchmrk_abs_multiyear.csv <schema>
TODO

log "Pipeline run complete (local phase). See TODO block above for cloud steps."
