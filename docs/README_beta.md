# RDM Datalab — Economic Benchmark Dataset (Beta)

## General Information
**Dataset name:** `econ_benchmrk_abs_qcew` (plus reference tables)  
**Maintainer / Contact:** RDM Datalab (datalab@rdm.org)  
**Date of release:** 2024-03-21  
**Geographic coverage:** United States — all counties with available ABS or QCEW data (includes "unspecified county" rollups where QCEW withholds a county)  
**Temporal coverage:** 2022 (aligned to the latest ABS and QCEW vintages; additional years will be appended as they are released)  
**NAICS vintage:** 2022 NAICS (2-digit sectors)  
**Data version:** 0.1 (Beta)  
**Contents:**
- Merged table: `econ_benchmrk_abs_qcew` — county × sector × year metrics combining ABS and QCEW plus derived wages/receipts  
- Reference table: `ref_state_cnty_uscb` — county / state / FIPS metadata + geography (lat/long) and placeholders for statewide/unspecified rows  
- Reference table: `ref_naics2_uscb` — 2-digit NAICS sector code → description mapping  
- Dashboard: public Looker Studio link for interactive exploration (maps, KPIs, county-detail cards)

**Intended use cases:**
- County-level economic benchmarking across sectors
- Cross-county comparisons of employment, payroll, wages, receipts per firm/per employee
- Economic development analysis, consulting, regional planning, or trend monitoring

**Limitations / Caveats:**
- ABS is a sample survey; Census suppresses (returns as null) county/sector metrics that could reveal individual respondents. QCEW is administrative UI payroll data that may not cover self-employed workers, military, etc. Nulls are expected where suppression occurs.  
- QCEW publishes "unspecified county" (`xx999`) rows when county assignment is withheld; ABS does not, so `abs_*` columns remain null for those rows.  
- NAICS2 aggregation can obscure variability within sectors. Users needing finer detail should treat these aggregates cautiously.  
- All monetary fields are nominal USD (not inflation-adjusted).  
- County boundaries and FIPS codes reflect the 2022 Gazetteer; boundary changes after that date may not appear.

---

## Data Files Overview

| File / Table | Description |
|--------------|-------------|
| `econ_benchmrk_abs_qcew.csv` | Main table: ABS + QCEW metrics + derived ratios at county × NAICS2 × year |
| `ref_state_cnty_uscb.csv` | County reference: FIPS, names, lat/long, land/water, synthetic statewide/unspecified rows |
| `ref_naics2_uscb.csv` | Sector reference: 2-digit NAICS code → description |
| Dashboard (public link) | Looker Studio dashboard for quick visualization (maps, KPI cards, sector tables) |

---

## Data Dictionary (`econ_benchmrk_abs_qcew`)

| Column | Description | Type | Units / Notes | Missing data handling |
|--------|-------------|------|----------------|-----------------------|
| `year_num` | Data year (YYYY) | INT64 | Calendar year | Required |
| `state_cnty_fips_cd` | 5-digit FIPS (state + county); `xx999` indicates unspecified county | STRING | NA | Required |
| `naics2_sector_cd` | Two-digit NAICS sector code | STRING | NA | Required |
| `naics2_sector_desc` | Sector description (from ref table) | STRING | NA | Null when ABS does not publish the sector for that row (e.g., QCEW-only rows) |
| `cnty_nm` | County or planning region name | STRING | NA | Null for unspecified or ABS-suppressed rows |
| `state_nm` | State name | STRING | NA | Optional |
| `cnty_full_nm` | County + state label (for dashboard display) | STRING | NA | Optional |
| `geo_id` | GEOID for county | STRING | NA | Null for unspecified rows |
| `ind_level_num` | Industry level (2 = NAICS2) | INT64 | NA | Null for QCEW-only rows |
| `abs_firm_num` | ABS firm count | INT64 | Count | Null when ABS suppresses the cell |
| `abs_emp_num` | ABS employment | INT64 | Count | Null when suppressed |
| `abs_payroll_usd_amt` | ABS payroll | NUMERIC | USD | Null when suppressed |
| `abs_rcpt_usd_amt` | ABS receipts | NUMERIC | USD | Null when suppressed |
| `abs_rcpt_per_emp_usd_amt` | Receipts per ABS employee | NUMERIC | USD per employee | Null if `abs_emp_num` ≤ 0 or suppressed |
| `abs_rcpt_per_firm_usd_amt` | Receipts per ABS firm | NUMERIC | USD per firm | Null if `abs_firm_num` ≤ 0 or suppressed |
| `abs_wage_per_emp_usd_amt` | Payroll per ABS employee | NUMERIC | USD per employee | Null if `abs_emp_num` ≤ 0 or suppressed |
| `qcew_ann_avg_emp_lvl_num` | QCEW average annual employment | INT64 | Count | Null/suppressed |
| `qcew_ttl_ann_wage_usd_amt` | QCEW total wages | NUMERIC | USD | Null/suppressed |
| `qcew_avg_wkly_wage_usd_amt` | QCEW average weekly wage | NUMERIC | USD/week | Null/suppressed |
| `qcew_wage_per_emp_usd_amt` | Derived QCEW annual wage per employee | NUMERIC | USD per employee | Null if `qcew_ann_avg_emp_lvl_num` ≤ 0 |

### Reference tables
**`ref_state_cnty_uscb`**: `state_cnty_fips_cd`, `state_cd`, `cnty_ansi_nm`, `cnty_nm`, `land_area_num`, `water_area_num`, `lat_num`, `long_num`. Includes synthetic statewide (`xx000`) and unspecified (`xx999`) rows and Connecticut planning regions.

**`ref_naics2_uscb`**: `naics2_sector_cd`, `naics2_sector_desc` for NAICS 2-digit sectors.

---

## Missing Data & Suppression Handling
- ABS suppresses small-count cells for confidentiality; such rows retain QCEW metrics but `abs_*` columns are null.
- QCEW "unspecified" rows (FIPS ending `999`) carry employment/wages but no ABS data; `cnty_nm`, `geo_id`, etc., are also null.
- Derived metrics are computed only when denominators are > 0 and non-null; otherwise they remain null to avoid misleading values.

---

## Usage Instructions
1. Load `econ_benchmrk_abs_qcew.csv` plus the reference tables into your analytics environment (pandas, R, SQL, etc.).
2. Join `ref_state_cnty_uscb` for names/lat-long and `ref_naics2_uscb` for sector descriptions as needed.
3. Filter on `year_num`, `state_cnty_fips_cd`, `naics2_sector_cd` to isolate geographies or sectors.
4. Handle nulls carefully—e.g., include/exclude `xx999` rows explicitly in statewide totals.
5. For quick exploration, use the bundled dashboard link.

---

## Versioning & Contact
- **Version:** 0.1 (Beta)  
- **Date:** 2024-03-21  
- **Maintainer:** RDM Datalab (datalab@rdm.org)  
- **Change log:**
  - v0.1 — Initial beta release with 2022 ABS/QCEW data, reference tables, and Looker dashboard link.
