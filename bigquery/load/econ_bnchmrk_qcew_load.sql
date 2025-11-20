bq load \
  --source_format=CSV \
  --field_delimiter=, \
  --encoding=UTF-8 \
  --skip_leading_rows=1 \
  rdm-datalab-portfolio:portfolio_data.econ_bnchmrk_qcew \
  gs://rdm_datalab_portfolio/econ_bnchmrk_qcew.csv \
  year_num:INT64,naics2_sector_cd:STRING,state_cnty_fips_cd:STRING,state_fips_cd:STRING,cnty_fips_cd:STRING,own_cd:STRING,qcew_ann_avg_emp_lvl_num:INT64,qcew_ttl_ann_wage_usd_amt:NUMERIC,qcew_avg_wkly_wage_usd_amt:NUMERIC
