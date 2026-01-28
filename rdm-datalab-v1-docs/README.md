# RDM Datalab v1 documentation pack (data-only product)

This folder contains v1 documentation intended to live in the `rdm-datalab-pipelines` repository.

## Contents

- `docs/qa/` — reconciliation methodology and QA artifacts
- `docs/data_product/` — end-user interpretation notes

## Minimum checklist before Gumroad data-only release

1. **Reconciliation**: at least 2 counties × 2 NAICS2 × 2 years pass for both ABS and QCEW.
2. **National snapshot**: store a timestamped national totals snapshot.
3. **Anomaly note**: document any known comparability issues and how the user should interpret trends.
4. **Packaging**: include schema, data dictionary, and “how to use” examples for BigQuery + CSV.

## Data dictionary

- `docs/data_product/data_dictionary.md` — column-by-column dictionary for the v1 data-only release.
