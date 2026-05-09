from unittest import TestCase, mock
import tempfile
import pandas
import os
import pytest
from process_report import process_report, util


class TestMonthUtils(TestCase):
    def test_get_month_diff(self):
        testcases = [
            (("2024-12", "2024-03"), 9),
            (("2024-12", "2023-03"), 21),
            (("2024-11", "2024-12"), -1),
            (("2024-12", "2025-03"), -3),
        ]
        for arglist, answer in testcases:
            assert util.get_month_diff(*arglist) == answer
        with pytest.raises(ValueError):
            util.get_month_diff("2024-16", "2025-03")


class TestMergeCSV(TestCase):
    def setUp(self):
        self.header = ["Cost", "Name", "Rate"]
        self.data = [
            [1, "Alice, Allison", 25],
            [2, "Bob", 30],
            [3, "Charlie", 28],
        ]

        self.csv_files = []

        for _ in range(3):
            csv_file = tempfile.NamedTemporaryFile(
                delete=False, mode="w", suffix=".csv"
            )
            self.csv_files.append(csv_file)
            dataframe = pandas.DataFrame(self.data, columns=self.header)
            dataframe.to_csv(csv_file, index=False, quotechar="|")
            csv_file.close()

    def tearDown(self):
        for csv_file in self.csv_files:
            os.remove(csv_file.name)

    def test_merge_csv(self):
        merged_dataframe = process_report.merge_csv(
            [csv_file.name for csv_file in self.csv_files]
        )

        expected_rows = len(self.data) * 3

        # `len` for a pandas dataframe excludes the header row
        assert len(merged_dataframe) == expected_rows

        # Assert that the headers in the merged DataFrame match the expected headers
        assert merged_dataframe.columns.tolist() == self.header

        assert merged_dataframe["Name"].iloc[0] == "Alice, Allison"


class TestValidateRequiredEnvVars(TestCase):
    @mock.patch.dict(
        "os.environ", {"KEYCLOAK_CLIENT_ID": "test", "KEYCLOAK_CLIENT_SECRET": "test"}
    )
    def test_env_vars_valid(self):
        process_report.validate_required_env_vars(
            ["KEYCLOAK_CLIENT_ID", "KEYCLOAK_CLIENT_SECRET"]
        )

    @mock.patch.dict("os.environ", {"KEYCLOAK_CLIENT_ID": "test"})
    def test_env_vars_missing(self):
        with pytest.raises(SystemExit):
            process_report.validate_required_env_vars(
                ["KEYCLOAK_CLIENT_ID", "KEYCLOAK_CLIENT_SECRET"]
            )
