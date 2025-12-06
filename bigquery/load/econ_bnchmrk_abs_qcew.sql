bq query \
  --use_legacy_sql=false \
  --replace=true \
  '
CREATE OR REPLACE TABLE `rdm-datalab-portfolio.portfolio_data.econ_bnchmrk_abs_qcew` AS
WITH abs_filtered AS (
  SELECT *
  FROM `rdm-datalab-portfolio.portfolio_data.econ_bnchmrk_abs`
  WHERE ind_level_num = 2
    AND CAST(naics2_sector_cd AS STRING) NOT IN ("00","99")
    AND REGEXP_CONTAINS(state_cnty_fips_cd, r"^\d{5}$")
),
qcew_filtered AS (
  SELECT *
  FROM `rdm-datalab-portfolio.portfolio_data.econ_bnchmrk_qcew`
  WHERE CAST(naics2_sector_cd AS STRING) NOT IN ("00","99")
    AND REGEXP_CONTAINS(state_cnty_fips_cd, r"^\d{5}$")
),
state_lookup AS (
  SELECT state_cd, state_nm
  FROM UNNEST([
    STRUCT("AL" AS state_cd, "Alabama" AS state_nm),
    STRUCT("AK", "Alaska"),
    STRUCT("AZ", "Arizona"),
    STRUCT("AR", "Arkansas"),
    STRUCT("CA", "California"),
    STRUCT("CO", "Colorado"),
    STRUCT("CT", "Connecticut"),
    STRUCT("DE", "Delaware"),
    STRUCT("DC", "District of Columbia"),
    STRUCT("FL", "Florida"),
    STRUCT("GA", "Georgia"),
    STRUCT("HI", "Hawaii"),
    STRUCT("ID", "Idaho"),
    STRUCT("IL", "Illinois"),
    STRUCT("IN", "Indiana"),
    STRUCT("IA", "Iowa"),
    STRUCT("KS", "Kansas"),
    STRUCT("KY", "Kentucky"),
    STRUCT("LA", "Louisiana"),
    STRUCT("ME", "Maine"),
    STRUCT("MD", "Maryland"),
    STRUCT("MA", "Massachusetts"),
    STRUCT("MI", "Michigan"),
    STRUCT("MN", "Minnesota"),
    STRUCT("MS", "Mississippi"),
    STRUCT("MO", "Missouri"),
    STRUCT("MT", "Montana"),
    STRUCT("NE", "Nebraska"),
    STRUCT("NV", "Nevada"),
    STRUCT("NH", "New Hampshire"),
    STRUCT("NJ", "New Jersey"),
    STRUCT("NM", "New Mexico"),
    STRUCT("NY", "New York"),
    STRUCT("NC", "North Carolina"),
    STRUCT("ND", "North Dakota"),
    STRUCT("OH", "Ohio"),
    STRUCT("OK", "Oklahoma"),
    STRUCT("OR", "Oregon"),
    STRUCT("PA", "Pennsylvania"),
    STRUCT("RI", "Rhode Island"),
    STRUCT("SC", "South Carolina"),
    STRUCT("SD", "South Dakota"),
    STRUCT("TN", "Tennessee"),
    STRUCT("TX", "Texas"),
    STRUCT("UT", "Utah"),
    STRUCT("VT", "Vermont"),
    STRUCT("VA", "Virginia"),
    STRUCT("WA", "Washington"),
    STRUCT("WV", "West Virginia"),
    STRUCT("WI", "Wisconsin"),
    STRUCT("WY", "Wyoming"),
    STRUCT("PR", "Puerto Rico"),
    STRUCT("VI", "U.S. Virgin Islands")
  ])
),
geo_ref AS (
  SELECT
    r.state_cnty_fips_cd,
    r.cnty_nm,
    COALESCE(s.state_nm, r.state_cd) AS state_nm,
    CONCAT(r.cnty_nm, ", ", COALESCE(s.state_nm, r.state_cd)) AS cnty_full_nm
  FROM `rdm-datalab-portfolio.portfolio_data.ref_state_cnty_uscb` AS r
  LEFT JOIN state_lookup AS s
    ON r.state_cd = s.state_cd
),
naics_ref AS (
  SELECT naics2_sector_cd, naics2_sector_desc
  FROM `rdm-datalab-portfolio.portfolio_data.ref_naics2_uscb`
)
SELECT
  COALESCE(a.state_cnty_fips_cd, q.state_cnty_fips_cd) AS state_cnty_fips_cd,
  COALESCE(a.naics2_sector_cd, q.naics2_sector_cd)     AS naics2_sector_cd,
  COALESCE(a.year_num, q.year_num)                     AS year_num,
  COALESCE(n.naics2_sector_desc, a.naics2_sector_desc, q.naics2_sector_cd) AS naics2_sector_desc,
  COALESCE(g.cnty_nm, a.cnty_nm) AS cnty_nm,
  g.state_nm,
  g.cnty_full_nm,
  a.geo_id,
  a.ind_level_num,
  a.abs_firm_num,
  a.abs_emp_num,
  a.abs_payroll_usd_amt,
  a.abs_rcpt_usd_amt,
  a.abs_rcpt_per_emp_usd_amt,
  q.qcew_ann_avg_emp_lvl_num,
  q.qcew_ttl_ann_wage_usd_amt,
  q.qcew_avg_wkly_wage_usd_amt,
  SAFE_DIVIDE(q.qcew_ttl_ann_wage_usd_amt, q.qcew_ann_avg_emp_lvl_num) AS qcew_wage_per_emp_usd_amt,
  SAFE_DIVIDE(a.abs_payroll_usd_amt, a.abs_emp_num)                     AS abs_wage_per_emp_usd_amt,
  a.abs_rcpt_per_firm_usd_amt
FROM abs_filtered AS a
FULL OUTER JOIN qcew_filtered AS q
  ON a.year_num = q.year_num
 AND a.state_cnty_fips_cd = q.state_cnty_fips_cd
 AND a.naics2_sector_cd = q.naics2_sector_cd
LEFT JOIN geo_ref AS g
  ON COALESCE(a.state_cnty_fips_cd, q.state_cnty_fips_cd) = g.state_cnty_fips_cd
LEFT JOIN naics_ref AS n
  ON COALESCE(a.naics2_sector_cd, q.naics2_sector_cd) = n.naics2_sector_cd
WHERE SUBSTR(COALESCE(a.state_cnty_fips_cd, q.state_cnty_fips_cd), 3, 3) <> "000";
'
