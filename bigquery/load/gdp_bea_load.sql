bq load \
  --source_format=CSV \
  --field_delimiter=, \
  --encoding=UTF-8 \
  --skip_leading_rows=1 \
  rdm-datalab-portfolio:portfolio_data.gdp_bea \
  gs://rdm_datalab_portfolio/gdp_bea.csv \
  year_num:INT64,naics2_sector_cd:STRING,state_cnty_fips_cd:STRING,line_cd:STRING,naics2_sector_desc:STRING,gdp_amt:NUMERIC
