CREATE OR REPLACE TABLE `rdm-datalab-portfolio.portfolio_data.econ_bnchmrk_abs_qcew_state_ttl`
(
  state_fips_cd               STRING,
  naics2_sector_cd            STRING,
  year_num                    INT64,
  naics2_sector_desc          STRING,
  abs_firm_num                INT64,
  abs_emp_num                 INT64,
  abs_payroll_usd_amt         NUMERIC,
  abs_rcpt_usd_amt            NUMERIC,
  abs_rcpt_per_emp_usd_amt    NUMERIC,
  qcew_ann_avg_emp_lvl_num    INT64,
  qcew_ttl_ann_wage_usd_amt   NUMERIC,
  qcew_avg_wkly_wage_usd_amt  NUMERIC,
  qcew_wage_per_emp_usd_amt   NUMERIC,
  abs_wage_per_emp_usd_amt    NUMERIC,
  abs_rcpt_per_firm_usd_amt   NUMERIC
);
