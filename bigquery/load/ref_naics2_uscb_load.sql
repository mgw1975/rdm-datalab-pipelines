bq query \
  --use_legacy_sql=false \
  --replace=true \
  --destination_table=rdm-datalab-portfolio:portfolio_data.ref_naics2_uscb \
  '
WITH base AS (
  SELECT *
  FROM `rdm-datalab-portfolio.portfolio_data.ref_naics2_uscb`
), extras AS (
  SELECT '00' AS naics2_sector_cd, 'Total for all sectors' AS naics2_sector_desc UNION ALL
  SELECT '99', 'Unclassified (suppression bucket)'
)
SELECT * FROM base
UNION ALL
SELECT * FROM extras
'
