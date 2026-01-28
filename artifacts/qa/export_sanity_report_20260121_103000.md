# Export Sanity Report (2026-01-21T10:30:00)

## Inputs
- Fact: `outputs/econ_bnchmrk_abs_qcew`
- NAICS ref: `outputs/ref_naics2_uscb`
- County ref: `outputs/ref_state_cnty_uscb`
- Run timestamp: `2026-01-21T10:30:00`

## Summary
| Severity | Passed | Failed |
| --- | --- | --- |
| ERROR | 20 | 4 |
| WARN | 9 | 2 |

## Key stats
- Rows: 119138
- Years: [2022, 2023]
- Rows by year: {2022: 59564, 2023: 59574}
- Distinct counties by year: {2022: 3283, 2023: 3283}
- Distinct NAICS2 by year: {2022: 20, 2023: 20}

## Duplicate keys
_None_

## Missing joins
| check | detail |
| --- | --- |
| join: county extra keys | 56 county keys unused by fact. |
| join: naics extra keys | 2 naics keys unused by fact. |

## Null-rate table (numeric columns)
| metric | null_pct | non_numeric_pct | scientific_strings |
| --- | --- | --- | --- |
| abs_payroll_usd_amt | 52.74 | 0.00 | 0 |
| abs_rcpt_usd_amt | 52.74 | 0.00 | 0 |

## Sample outliers (top 10 by year)
### abs_rcpt_usd_amt
| year | state_cnty_fips_cd | naics2_sector_cd | value |
| --- | --- | --- | --- |
| 2022 | 48201 | 42 | 728128343000.0 |
| 2022 | 36061 | 52 | 484061993000.0 |
| 2022 | 06037 | 42 | 471594725000.0 |
| 2022 | 17031 | 42 | 289976680000.0 |
| 2022 | 48113 | 42 | 284791437000.0 |
| 2022 | 06059 | 42 | 226628358000.0 |
| 2022 | 06037 | 44-45 | 214835089000.0 |
| 2022 | 06085 | 51 | 203195409000.0 |
| 2022 | 06085 | 42 | 199410990000.0 |
| 2022 | 36061 | 51 | 194650967000.0 |
| 2023 | 48201 | 42 | 761569922000.0 |
| 2023 | 36061 | 52 | 511835503000.0 |
| 2023 | 06037 | 42 | 280577238000.0 |
| 2023 | 17031 | 42 | 221322909000.0 |
| 2023 | 06085 | 51 | 201410326000.0 |
| 2023 | 06037 | 44-45 | 187572598000.0 |
| 2023 | 48113 | 42 | 185577467000.0 |
| 2023 | 36061 | 51 | 174537637000.0 |
| 2023 | 06059 | 42 | 164754034000.0 |
| 2023 | 48201 | 31-33 | 164692180000.0 |

### abs_payroll_usd_amt
| year | state_cnty_fips_cd | naics2_sector_cd | value |
| --- | --- | --- | --- |
| 2022 | 36061 | 52 | 106023053000.0 |
| 2022 | 36061 | 54 | 55756515000.0 |
| 2022 | 06085 | 51 | 55659564000.0 |
| 2022 | 36061 | 51 | 41327021000.0 |
| 2022 | 06037 | 54 | 38050904000.0 |
| 2022 | 06085 | 55 | 36901449000.0 |
| 2022 | 06037 | 62 | 36010767000.0 |
| 2022 | 06081 | 51 | 33024714000.0 |
| 2022 | 17031 | 54 | 32504242000.0 |
| 2022 | 06075 | 51 | 31700268000.0 |
| 2023 | 36061 | 52 | 106351268000.0 |
| 2023 | 06085 | 51 | 67277872000.0 |
| 2023 | 36061 | 54 | 55068380000.0 |
| 2023 | 36061 | 51 | 42000319000.0 |
| 2023 | 06081 | 51 | 40639890000.0 |
| 2023 | 06037 | 62 | 39761811000.0 |
| 2023 | 06085 | 55 | 38290708000.0 |
| 2023 | 53033 | 51 | 37521122000.0 |
| 2023 | 17031 | 54 | 35413124000.0 |
| 2023 | 06037 | 54 | 34206233000.0 |

## Check details
| severity | check | status | detail |
| --- | --- | --- | --- |
| ERROR | fact: CSV parses cleanly | PASS | Parsed without malformed rows. |
| ERROR | fact: delimiter sanity | PASS | Parsed 50 columns. |
| ERROR | fact: non-empty headers | PASS | All column names present. |
| ERROR | fact: unique headers | PASS | Column names are unique. |
| ERROR | naics: CSV parses cleanly | PASS | Parsed without malformed rows. |
| ERROR | naics: delimiter sanity | PASS | Parsed 2 columns. |
| ERROR | naics: non-empty headers | PASS | All column names present. |
| ERROR | naics: unique headers | PASS | Column names are unique. |
| ERROR | county: CSV parses cleanly | PASS | Parsed without malformed rows. |
| ERROR | county: delimiter sanity | PASS | Parsed 10 columns. |
| ERROR | county: non-empty headers | PASS | All column names present. |
| ERROR | county: unique headers | PASS | Column names are unique. |
| ERROR | fact: required columns | FAIL | Missing columns: abs_firms, abs_emp, qcew_emp, qcew_wages_usd, qcew_avg_weekly_wage_usd |
| ERROR | naics: required columns | FAIL | Missing columns: naics2_sector_name |
| ERROR | county: required columns | FAIL | Missing columns: state_fips, county_fips, county_name |
| ERROR | county: county_name present | FAIL | Missing required column: county_name |
| ERROR | fact: state_cnty_fips_cd format | PASS | All FIPS codes are 5 digits. |
| ERROR | fact: year_num parse | PASS | All year_num values parse numeric. |
| ERROR | fact: year_num integer | PASS | All year_num values are integers. |
| WARN | fact: year_num expected set | PASS | Only expected years present. |
| ERROR | fact: naics2_sector_cd non-null | PASS | NAICS2 codes present. |
| WARN | fact: numeric parse summary | PASS | Numeric columns parsed (see report). |
| WARN | fact: naics2_sector_cd set | PASS | NAICS2 codes match reference. |
| ERROR | fact: duplicate keys | PASS | No duplicate keys. |
| ERROR | join: fact -> county | PASS | All fact FIPS found in county ref. |
| WARN | join: county extra keys | FAIL | 56 county keys unused by fact. |
| ERROR | join: fact -> naics | PASS | All fact NAICS codes in naics ref. |
| WARN | join: naics extra keys | FAIL | 2 naics keys unused by fact. |
| WARN | coverage: distinct counties 2022 | PASS | 3283 counties (expected 3283). |
| WARN | coverage: distinct counties 2023 | PASS | 3283 counties (expected 3283). |
| WARN | coverage: distinct naics2 2022 | PASS | 20 NAICS2 codes (expected 20). |
| WARN | coverage: distinct naics2 2023 | PASS | 20 NAICS2 codes (expected 20). |
| ERROR | fact: negative firms/emp | PASS | No negative firm/emp values. |
| WARN | fact: negative dollar values | PASS | No negative dollar values. |
| WARN | formatting: scientific notation strings | PASS | No scientific notation strings. |
| WARN | outliers: sample top 10 | PASS | Outliers sampled in report. |
