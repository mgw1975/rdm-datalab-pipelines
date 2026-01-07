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
  population_num             INT64,
  population_year            INT64,

  -- ABS measures
  abs_firm_num               INT64,
  abs_firm_prev_year_num     INT64,
  abs_emp_num                INT64,
  abs_emp_prev_year_num      INT64,
  abs_payroll_usd_amt        NUMERIC,
  abs_payroll_prev_year_usd_amt NUMERIC,
  abs_rcpt_usd_amt           NUMERIC,
  abs_rcpt_prev_year_usd_amt NUMERIC,
  abs_rcpt_per_emp_usd_amt   NUMERIC,
  cnty_abs_firm_num          INT64,
  cnty_abs_firm_prev_year_num INT64,
  cnty_firm_cncntrtn_idx     NUMERIC,

  -- QCEW measures
  qcew_ann_avg_emp_lvl_num   INT64,
  qcew_ann_avg_emp_prev_year_num INT64,
  qcew_ttl_ann_wage_usd_amt  NUMERIC,
  qcew_ttl_ann_wage_prev_year_usd_amt NUMERIC,
  qcew_avg_wkly_wage_usd_amt NUMERIC,

  -- Derived metrics
  qcew_wage_per_emp_usd_amt  NUMERIC,
  abs_wage_per_emp_usd_amt   NUMERIC,
  abs_rcpt_per_firm_usd_amt  NUMERIC,

  -- State totals (non-additive context)
  state_abs_firm_num              INT64,
  state_abs_firm_prev_year_num    INT64,
  state_abs_emp_num               INT64,
  state_abs_emp_prev_year_num     INT64,
  state_abs_payroll_usd_amt       NUMERIC,
  state_abs_payroll_prev_year_usd_amt NUMERIC,
  state_abs_rcpt_usd_amt          NUMERIC,
  state_abs_rcpt_prev_year_usd_amt NUMERIC,
  state_qcew_ann_avg_emp_lvl_num  INT64,
  state_qcew_ann_avg_emp_prev_year_num INT64,
  state_qcew_ttl_ann_wage_usd_amt NUMERIC,
  state_qcew_ttl_ann_wage_prev_year_usd_amt NUMERIC,

  state_abs_firm_rank_num          INT64,
  state_abs_emp_rank_num           INT64,
  state_abs_payroll_rank_num       INT64,
  state_abs_rcpt_rank_num          INT64,
  state_qcew_emp_rank_num          INT64,
  state_qcew_wage_rank_num         INT64
);
