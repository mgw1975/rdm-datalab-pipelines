CREATE TABLE `rdm-datalab-portfolio.portfolio_data.tri_epa`
(
  state_cd            STRING,  -- 2-char state code
  cnty_nm             STRING,  -- county name
  state_cnty_fips_cd  STRING,  -- 5-digit county FIPS
  naics2_sector_cd    STRING,  -- 2-digit NAICS sector
  tri_ttl_rls_lbs_amt NUMERIC  -- total releases (lbs)
);
