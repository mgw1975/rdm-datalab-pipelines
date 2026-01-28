# ABS 2022→2023 anomaly investigation playbook (v1)

Goal: determine whether the large national YoY declines in ABS totals from 2022 to 2023 reflect:
1) a true ABS published change,
2) a known methodology/coverage revision,
3) a pipeline scope issue.

## Phase 1 — Confirm definitions / year mapping

1. Confirm the ABS year mapping:
   - ABS survey year uses collection year; reference year is prior year.
   - Confirm which API vintage corresponds to which reference year.

2. In API pulls, include `YEAR` explicitly in the `get=` list where applicable to ensure you are pulling what you think.

## Phase 2 — Validate against Census-published totals

For each reference year:
- Pull Census-published “Company Summary” totals for:
  - firms, employment, payroll, receipts (or “sales/receipts”)
- Compare those totals to your:
  - (a) national total from summing counties × NAICS2
  - (b) any “US:*” API pull at total industry

Key check:
- Does `for=us:*` with the appropriate “total industry” group yield ~5.9M employer firms (per Census press releases)?

## Phase 3 — Identify whether totals are missing a slice

Compute contribution by:
- NAICS2 sector (you did this; the decline is broad)
- Geography class:
  - US total vs sum(counties)
  - states total vs sum(counties within state)
- Any additional dimensions defaulting to non-total values:
  - SEX, ETH_GROUP, RACE_GROUP, VET_GROUP, EMPSZFI
  - Ensure your pipeline explicitly sets these to “all/total” where needed.

## Phase 4 — Compare to other external anchors

Use independent series to sanity check:
- QCEW national employment & wages (already increasing in your snapshot)
- CBP / SUSB employer firm counts (if available for comparable years)
- BEA GDP / receipts proxies at high level

## Phase 5 — Document the outcome

Regardless of root cause, record:
- what was tested
- what data sources/links were used
- how users should interpret 2022→2023 ABS changes
