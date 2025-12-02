bq query \
  --use_legacy_sql=false \
  --replace=true \
  --destination_table=rdm-datalab-portfolio:portfolio_data.econ_bnchmrk_abs_qcew_indstr_ttl \
  '
WITH abs_totals AS (
  SELECT *
  FROM `rdm-datalab-portfolio.portfolio_data.econ_bnchmrk_abs`
  WHERE CAST(naics2_sector_cd AS STRING) IN ("00","99")
),
qcew_totals AS (
  SELECT *
  FROM `rdm-datalab-portfolio.portfolio_data.econ_bnchmrk_qcew`
  WHERE CAST(naics2_sector_cd AS STRING) IN ("00","99")
)
SELECT
  a.year_num,
  a.naics2_sector_cd,
  a.state_cnty_fips_cd,
  a.cnty_nm,
  a.geo_id,
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
FROM abs_totals AS a
LEFT JOIN qcew_totals AS q
  ON a.year_num = q.year_num
 AND a.state_cnty_fips_cd = q.state_cnty_fips_cd
 AND a.naics2_sector_cd = q.naics2_sector_cd;
'
