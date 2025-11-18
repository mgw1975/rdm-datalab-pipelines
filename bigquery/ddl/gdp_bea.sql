CREATE TABLE `rdm-datalab-portfolio.portfolio_data.gdp_bea`
(
  state_county_fips_cd STRING NOT NULL,
  line_cd STRING NOT NULL,
  naics2_sector_cd STRING NOT NULL,
  naics2_sector_desc STRING,
  gdp_2022_amt NUMERIC,
  gdp_2021_amt NUMERIC,
  gdp_2020_amt NUMERIC
);
