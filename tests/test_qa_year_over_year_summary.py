import os
import subprocess
import unittest


class TestQaYearOverYearSummary(unittest.TestCase):
    def test_script_runs_when_credentials_available(self) -> None:
        if not os.environ.get("GOOGLE_APPLICATION_CREDENTIALS") and not os.environ.get(
            "GOOGLE_CLOUD_PROJECT"
        ):
            self.skipTest("No BigQuery credentials configured in environment.")

        result = subprocess.run(
            ["python", "scripts/qa_year_over_year_summary.py"],
            capture_output=True,
            text=True,
        )
        if result.returncode != 0:
            self.fail(f"Script failed:\nSTDOUT:\n{result.stdout}\nSTDERR:\n{result.stderr}")


if __name__ == "__main__":
    unittest.main()
