bq load \
  --source_format=CSV \
  --field_delimiter=, \
  --encoding=UTF-8 \
  --skip_leading_rows=1 \
  rdm-datalab-portfolio:portfolio_data.ref_state_cnty_uscb \
  gs://rdm_datalab_portfolio/ref_state_cnty_uscb.csv \
  state_cnty_fips_cd:STRING,state_cd:STRING,cnty_ansi_nm:STRING,cnty_nm:STRING,land_area_num:INT64,water_area_num:INT64,lat_num:FLOAT64,long_num:FLOAT64
