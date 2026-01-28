import unittest

from pathlib import Path

import pandas as pd

from qa import qcew_reconciliation


class TestQcewReconciliation(unittest.TestCase):
    def test_load_qcew_source_fixture(self) -> None:
        config = qcew_reconciliation.QcewConfig(
            years=[2022],
            counties=["06075", "06085"],
            naics=["42", "62"],
            outdir=Path("artifacts/qa"),
            publish_bq=False,
            bq_table="unused",
            raw_template="tests/fixtures/qcew_singlefile_sample.csv",
            cache_dir=Path("data_raw/qcew/source_qa"),
            ownership_code="5",
            agg_level="74",
            allow_wage_tolerance=True,
            rdm_csv=None,
        )
        source = qcew_reconciliation.load_qcew_source(config)
        self.assertEqual(len(source), 2)
        self.assertIn("naics2_sector_cd", source.columns)
        self.assertIn("state_cnty_fips_cd", source.columns)

    def test_suppression_handling(self) -> None:
        source_df = pd.DataFrame(
            [
                {
                    "year": "2022",
                    "state_cnty_fips_cd": "06085",
                    "state_fips": "06",
                    "county_fips": "085",
                    "naics2_sector_cd": "62",
                    "annual_avg_emplvl": "D",
                    "total_annual_wages": "D",
                    "annual_avg_wkly_wage": "D",
                }
            ]
        )
        rdm_df = pd.DataFrame(
            [
                {
                    "year_num": 2022,
                    "state_cnty_fips_cd": "06085",
                    "naics2_sector_cd": "62",
                    "rdm_qcew_emp": 100,
                    "rdm_qcew_wages_usd": 5200000,
                    "rdm_qcew_avg_weekly_wage_usd": 1000,
                }
            ]
        )
        reconciled = qcew_reconciliation.reconcile_qcew(source_df, rdm_df, allow_wage_tolerance=True)
        row = reconciled.iloc[0]
        self.assertIn("source_suppressed", row["notes"])
        self.assertTrue(pd.isna(row["pass_all"]))

    def test_avg_weekly_wage_tolerance(self) -> None:
        source_df = pd.DataFrame(
            [
                {
                    "year": "2022",
                    "state_cnty_fips_cd": "06075",
                    "state_fips": "06",
                    "county_fips": "075",
                    "naics2_sector_cd": "42",
                    "annual_avg_emplvl": "100",
                    "total_annual_wages": "5200000",
                    "annual_avg_wkly_wage": "1000",
                }
            ]
        )
        rdm_df = pd.DataFrame(
            [
                {
                    "year_num": 2022,
                    "state_cnty_fips_cd": "06075",
                    "naics2_sector_cd": "42",
                    "rdm_qcew_emp": 100,
                    "rdm_qcew_wages_usd": 5200000,
                    "rdm_qcew_avg_weekly_wage_usd": 1001,
                }
            ]
        )
        reconciled = qcew_reconciliation.reconcile_qcew(source_df, rdm_df, allow_wage_tolerance=True)
        row = reconciled.iloc[0]
        self.assertTrue(row["pass_avg_weekly_wage"])


if __name__ == "__main__":
    unittest.main()
