CREATE OR REPLACE TABLE `rdm-datalab-portfolio.portfolio_data.gdp_bea`
(
  year_num             INT64,
  naics2_sector_cd     STRING,  -- 2-digit NAICS sector code
  state_cnty_fips_cd   STRING,  -- 5-digit county FIPS
  line_cd              STRING,  -- BEA line code
  naics2_sector_desc   STRING,  -- BEA description
  gdp_amt              NUMERIC  -- GDP (USD)
);
