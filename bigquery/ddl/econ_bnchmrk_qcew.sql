CREATE TABLE `rdm-datalab-portfolio.portfolio_data.econ_bnchmrk_qcew`
(
  state_fips_cd STRING NOT NULL,
  cnty_fips_cd STRING NOT NULL,
  naics2_sector_cd STRING NOT NULL,
  yr_num INT64,
  qcew_ann_avg_emp_lvl_num INT64,
  qcew_ttl_ann_wage_usd_amt NUMERIC,
  qcew_avg_wkly_wage_usd_amt NUMERIC
);
