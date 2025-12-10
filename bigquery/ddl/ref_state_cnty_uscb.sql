CREATE TABLE `rdm-datalab-portfolio.portfolio_data.ref_state_cnty_uscb`
(
  state_cnty_fips_cd STRING OPTIONS(description="5-digit FIPS state+county code"),
  state_cd STRING OPTIONS(description="State postal abbreviation"),
  cnty_ansi_nm STRING OPTIONS(description="ANSI county identifier"),
  cnty_nm STRING OPTIONS(description="County name"),
  land_area_num INT64 OPTIONS(description="Land area (square meters)"),
  water_area_num INT64 OPTIONS(description="Water area (square meters)"),
  lat_num FLOAT64 OPTIONS(description="Latitude of internal point"),
  long_num FLOAT64 OPTIONS(description="Longitude of internal point"),
  population_num INT64 OPTIONS(description="Population count from ACS"),
  population_year INT64 OPTIONS(description="ACS population vintage year")
);
