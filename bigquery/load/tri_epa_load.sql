bq load \
  --source_format=CSV \
  --field_delimiter=, \
  --encoding=UTF-8 \
  --skip_leading_rows=1 \
  rdm-datalab-portfolio:portfolio_data.tri_epa \
  gs://rdm_datalab_portfolio/tri_epa.csv \
  state_cd:STRING,cnty_nm:STRING,state_cnty_fips_cd:STRING,naics2_sector_cd:STRING,tri_ttl_rls_lbs_amt:NUMERIC
