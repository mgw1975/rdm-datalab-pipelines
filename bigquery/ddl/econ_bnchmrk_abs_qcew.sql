CREATE OR REPLACE TABLE `rdm-datalab-portfolio.portfolio_data.econ_bnchmrk_abs_qcew`
(
  -- Keys
  state_cnty_fips_cd         STRING,
  naics2_sector_cd           STRING,
  year_num                   INT64,

  -- Descriptive
  naics2_sector_desc         STRING,
  cnty_nm                    STRING,
  state_nm                   STRING,
  cnty_full_nm               STRING,
  geo_id                     STRING,
  ind_level_num              INT64,

  -- ABS measures
  abs_firm_num               INT64,
  abs_emp_num                INT64,
  abs_payroll_usd_amt        NUMERIC,
  abs_rcpt_usd_amt           NUMERIC,
  abs_rcpt_per_emp_usd_amt   NUMERIC,

  -- QCEW measures
  qcew_ann_avg_emp_lvl_num   INT64,
  qcew_ttl_ann_wage_usd_amt  NUMERIC,
  qcew_avg_wkly_wage_usd_amt NUMERIC,

  -- Derived metrics
  qcew_wage_per_emp_usd_amt  NUMERIC,
  abs_wage_per_emp_usd_amt   NUMERIC,
  abs_rcpt_per_firm_usd_amt  NUMERIC
);
