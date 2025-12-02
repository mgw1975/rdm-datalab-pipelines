CREATE OR REPLACE TABLE `rdm-datalab-portfolio.portfolio_data.econ_bnchmrk_2022_abs`
(
  year_num                  INT64,    -- ABS year
  state_cnty_fips_cd        STRING,   -- 5-digit county FIPS
  naics2_sector_cd          STRING,   -- NAICS 2022 sector code
  cnty_nm                   STRING,   -- county name
  geo_id                    STRING,   -- ABS GEOID
  naics2_sector_desc        STRING,   -- NAICS 2022 sector description
  ind_level_num             INT64,    -- industry level
  state_fips_cd             STRING,   -- 2-digit state FIPS
  cnty_fips_cd              STRING,   -- 3-digit county FIPS
  abs_firm_num              INT64,    -- firm count
  abs_emp_num               INT64,    -- employment count
  abs_payroll_usd_amt       NUMERIC,  -- payroll (USD)
  abs_rcpt_usd_amt          NUMERIC,  -- total receipts (USD)
  abs_rcpt_per_emp_usd_amt  NUMERIC   -- receipts per employee (USD)
);
