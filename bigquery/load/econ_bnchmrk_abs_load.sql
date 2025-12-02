bq load \
  --source_format=CSV \
  --field_delimiter=, \
  --encoding=UTF-8 \
  --skip_leading_rows=1 \
  rdm-datalab-portfolio:portfolio_data.econ_bnchmrk_abs \
  gs://rdm_datalab_portfolio/econ_bnchmrk_abs.csv \
  year_num:INT64,state_cnty_fips_cd:STRING,naics2_sector_cd:STRING,cnty_nm:STRING,geo_id:STRING,naics2_sector_desc:STRING,ind_level_num:INT64,abs_firm_num:INT64,abs_emp_num:INT64,abs_payroll_usd_amt:NUMERIC,abs_rcpt_usd_amt:NUMERIC,abs_rcpt_per_emp_usd_amt:NUMERIC
