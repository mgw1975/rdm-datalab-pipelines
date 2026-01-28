# Export Sanity Check (Offline QA)

Purpose: quick, buyer-facing validation of export CSVs before upload (schema drift, duplicates, join integrity, formatting, coverage).

## Run

```bash
python -m qa.export_sanity_check \
  --fact outputs/econ_bnchmrk_abs_qcew \
  --naics outputs/ref_naics2_uscb \
  --county outputs/ref_state_cnty_uscb \
  --outdir artifacts/qa
```

## Outputs

- `artifacts/qa/export_sanity_report_<timestamp>.md`
- `artifacts/qa/export_sanity_report_<timestamp>.json`

Exit code is non-zero only if any `ERROR` checks fail.

## Common expected WARNs

- `join: county extra keys` may flag county reference rows not present in the fact export (e.g., territories or non-county entities).
- `join: naics extra keys` may flag NAICS2 codes in the reference table not used in a given fact release.

Adjust `COLUMN_MAP` in `qa/export_sanity_check.py` if column names drift.
