bq query \
  --use_legacy_sql=false \
  --replace=true \
  --destination_table=rdm-datalab-portfolio:portfolio_data.econ_bnchmrk_abs_qcew_state_ttl \
  '
WITH abs_state AS (
  SELECT
    SUBSTR(state_cnty_fips_cd, 1, 2) AS state_fips_cd,
    naics2_sector_cd,
    year_num,
    ANY_VALUE(naics2_sector_desc) AS naics2_sector_desc,
    SUM(abs_firm_num) AS abs_firm_num,
    SUM(abs_emp_num) AS abs_emp_num,
    SUM(abs_payroll_usd_amt) AS abs_payroll_usd_amt,
    SUM(abs_rcpt_usd_amt) AS abs_rcpt_usd_amt,
    SAFE_DIVIDE(SUM(abs_rcpt_usd_amt), NULLIF(SUM(abs_emp_num), 0)) AS abs_rcpt_per_emp_usd_amt
  FROM `rdm-datalab-portfolio.portfolio_data.econ_bnchmrk_abs`
  WHERE ind_level_num = 2
  GROUP BY state_fips_cd, naics2_sector_cd, year_num
),
qcew_state AS (
  SELECT
    SUBSTR(state_cnty_fips_cd, 1, 2) AS state_fips_cd,
    naics2_sector_cd,
    year_num,
    SUM(qcew_ann_avg_emp_lvl_num) AS qcew_ann_avg_emp_lvl_num,
    SUM(qcew_ttl_ann_wage_usd_amt) AS qcew_ttl_ann_wage_usd_amt,
    SAFE_DIVIDE(SUM(qcew_ttl_ann_wage_usd_amt), NULLIF(SUM(qcew_ann_avg_emp_lvl_num), 0)) / 52 AS qcew_avg_wkly_wage_usd_amt
  FROM `rdm-datalab-portfolio.portfolio_data.econ_bnchmrk_qcew`
  GROUP BY state_fips_cd, naics2_sector_cd, year_num
)
SELECT
  a.state_fips_cd,
  a.naics2_sector_cd,
  a.year_num,
  a.naics2_sector_desc,
  a.abs_firm_num,
  a.abs_emp_num,
  a.abs_payroll_usd_amt,
  a.abs_rcpt_usd_amt,
  a.abs_rcpt_per_emp_usd_amt,
  q.qcew_ann_avg_emp_lvl_num,
  q.qcew_ttl_ann_wage_usd_amt,
  q.qcew_avg_wkly_wage_usd_amt,
  SAFE_DIVIDE(q.qcew_ttl_ann_wage_usd_amt, q.qcew_ann_avg_emp_lvl_num) AS qcew_wage_per_emp_usd_amt,
  SAFE_DIVIDE(a.abs_payroll_usd_amt, a.abs_emp_num)                      AS abs_wage_per_emp_usd_amt,
  SAFE_DIVIDE(a.abs_rcpt_usd_amt, a.abs_firm_num)                        AS abs_rcpt_per_firm_usd_amt
FROM abs_state AS a
LEFT JOIN qcew_state AS q
  ON a.state_fips_cd = q.state_fips_cd
 AND a.naics2_sector_cd = q.naics2_sector_cd
 AND a.year_num = q.year_num;
'
