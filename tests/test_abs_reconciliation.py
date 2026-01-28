import unittest
from unittest.mock import patch

import pandas as pd

from qa import abs_reconciliation


class TestAbsReconciliation(unittest.TestCase):
    def test_parse_census_payload(self) -> None:
        payload = '[["NAICS2022","NAME","FIRMPDEMP","EMP","PAYANN","RCPPDEMP","state","county"],' \
                  '["42","Sample County","10","100","200","300","06","075"]]'
        parsed = abs_reconciliation.parse_census_payload(payload)
        self.assertEqual(parsed["NAICS2022"], "42")
        self.assertEqual(parsed["FIRMPDEMP"], "10")

    def test_census_scaling(self) -> None:
        fake_record = {
            "FIRMPDEMP": "10",
            "EMP": "100",
            "PAYANN": "200",
            "RCPPDEMP": "300",
        }
        with patch.object(abs_reconciliation, "_fetch_census_slice", return_value=fake_record):
            df = abs_reconciliation.fetch_census_data([2022], ["06075"], ["42"])
        row = df.iloc[0]
        self.assertEqual(row["source_census_payann_usd"], 200000)
        self.assertEqual(row["source_census_rcppdemp_usd"], 300000)

    def test_tolerance_logic(self) -> None:
        census_df = pd.DataFrame(
            [
                {
                    "year_num": 2022,
                    "state_cnty_fips_cd": "06075",
                    "state_fips": "06",
                    "county_fips": "075",
                    "naics2_sector_cd": "42",
                    "source_census_firmpdemp": 10,
                    "source_census_emp": 100,
                    "source_census_payann_usd": 200000,
                    "source_census_rcppdemp_usd": 300000,
                    "notes": "",
                }
            ]
        )
        rdm_df = pd.DataFrame(
            [
                {
                    "year_num": 2022,
                    "state_cnty_fips_cd": "06075",
                    "naics2_sector_cd": "42",
                    "rdm_abs_firms": 10,
                    "rdm_abs_emp": 100,
                    "rdm_abs_payroll_usd_amt": 200500,
                    "rdm_abs_rcpt_usd_amt": 302500,
                }
            ]
        )
        reconciled = abs_reconciliation.reconcile_abs(census_df, rdm_df)
        row = reconciled.iloc[0]
        self.assertTrue(row["pass_firms"])
        self.assertTrue(row["pass_emp"])
        self.assertTrue(row["pass_payroll"])
        self.assertFalse(row["pass_receipts"])


if __name__ == "__main__":
    unittest.main()
