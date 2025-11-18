CREATE VIEW `rdm-datalab-portfolio.portfolio_data.wages_by_state`
AS
SELECT
  t2.state_cd,
  ROUND(AVG(t1.avg_weekly_wage_usd_num), 2) AS state_avg_weekly_wage_usd_num
FROM `rdm-datalab-portfolio.portfolio_data.portfolio_abs_qcew_ca_county_naics2` AS t1
LEFT JOIN `rdm-datalab-portfolio.portfolio_data.us_state_county_derived` AS t2
  ON t1.state_fips_cd = t2.state_fips_cd
 AND t1.county_fips_cd = t2.county_fips_cd
WHERE t1.naics2022_cd != '99'
GROUP BY 1;
