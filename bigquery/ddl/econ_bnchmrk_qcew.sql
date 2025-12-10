CREATE OR REPLACE TABLE `rdm-datalab-portfolio.portfolio_data.econ_bnchmrk_qcew`
(
  year_num                    INT64,             -- reference year (e.g., 2022)
  naics2_sector_cd            STRING,            -- 2-digit NAICS sector
  state_cnty_fips_cd          STRING,            -- 5-digit county FIPS
  state_fips_cd               STRING,             -- 2-digit state FIPS
  cnty_fips_cd                STRING,             -- 3-digit county FIPS
  own_cd                      STRING,             -- ownership code (0 total, 5 private, etc.)
  qcew_ann_avg_emp_lvl_num    INT64,              -- annual average employment level
  qcew_ttl_ann_wage_usd_amt   NUMERIC,            -- total annual wages (USD)
  qcew_avg_wkly_wage_usd_amt  NUMERIC             -- average weekly wage (USD)
);
