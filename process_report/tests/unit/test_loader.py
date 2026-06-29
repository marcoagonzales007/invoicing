import os
import tempfile
from unittest import TestCase, mock
import pandas
from process_report.invoices import invoice
from process_report.loader import Loader
from process_report.settings import invoice_settings


class TestLoader(TestCase):
    def _write_temp_yaml(self, content):
        """Write content to a temp YAML file and return its path."""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            f.write(content)
            return f.name

    def test_load_pi_config_valid(self):
        """A YAML list is parsed into a list of dicts."""
        filepath = self._write_temp_yaml("- username: pi1\n- username: pi2\n")
        loader = Loader()
        result = loader._load_pi_config(filepath)
        os.remove(filepath)
        assert result == [{"username": "pi1"}, {"username": "pi2"}]

    def test_load_pi_config_not_list(self):
        """A YAML file that isn't a list raises ValueError."""
        filepath = self._write_temp_yaml("username: pi1\n")  # a dict, not a list
        loader = Loader()
        with self.assertRaises(ValueError):
            loader._load_pi_config(filepath)
        os.remove(filepath)

    def test_get_nonbillable_pis(self):
        """PIs without non_billed_su_types are returned as nonbillable."""
        filepath = self._write_temp_yaml(
            "- username: pi1\n"
            "- username: pi2\n"
            "  non_billed_su_types:\n"
            "    - name: GPU\n"
        )
        loader = Loader()
        with mock.patch.object(invoice_settings, "nonbillable_pis_filepath", filepath):
            result = loader.get_nonbillable_pis()
        os.remove(filepath)
        assert result == ["pi1"]

    def test_get_pi_non_billed_su_types(self):
        """PIs with non_billed_su_types map to their list of SU type names."""
        filepath = self._write_temp_yaml(
            "- username: pi1\n"
            "- username: pi2\n"
            "  non_billed_su_types:\n"
            "    - name: GPU\n"
        )
        loader = Loader()
        with mock.patch.object(invoice_settings, "nonbillable_pis_filepath", filepath):
            result = loader.get_pi_non_billed_su_types()
        os.remove(filepath)
        assert result == {"pi2": ["GPU"]}

    def test_get_nonbillable_projects_timed_with_clusters(self):
        filepath = self._write_temp_yaml(
            "- name: ProjectA\n"
            "  start: 2025-01\n"
            "  end: 2025-12\n"
            "  clusters:\n"
            "    - name: stack\n"
        )
        loader = Loader()
        with (
            mock.patch.object(
                invoice_settings, "nonbillable_projects_filepath", filepath
            ),
            mock.patch.object(invoice_settings, "invoice_month", "2025-06"),
        ):
            result = loader.get_nonbillable_projects()
        os.remove(filepath)

        answer = pandas.DataFrame(
            [("ProjectA", "stack", True, False)],
            columns=[
                invoice.NONBILLABLE_PROJECT_NAME,
                invoice.NONBILLABLE_CLUSTER_NAME,
                invoice.NONBILLABLE_IS_TIMED,
                invoice.NONBILLABLE_IS_BILLABLE_OVERRIDE,
            ],
        )
        assert result.equals(answer)

    def test_get_nonbillable_projects_timed_no_clusters(self):
        """A timed project in range with no clusters uses None for the cluster."""
        filepath = self._write_temp_yaml(
            "- name: ProjectA\n  start: 2025-01\n  end: 2025-12\n"
        )
        loader = Loader()
        with (
            mock.patch.object(
                invoice_settings, "nonbillable_projects_filepath", filepath
            ),
            mock.patch.object(invoice_settings, "invoice_month", "2025-06"),
        ):
            result = loader.get_nonbillable_projects()
        os.remove(filepath)

        answer = pandas.DataFrame(
            [("ProjectA", None, True, False)],
            columns=[
                invoice.NONBILLABLE_PROJECT_NAME,
                invoice.NONBILLABLE_CLUSTER_NAME,
                invoice.NONBILLABLE_IS_TIMED,
                invoice.NONBILLABLE_IS_BILLABLE_OVERRIDE,
            ],
        )
        assert result.equals(answer)

    def test_get_alias_map(self):
        loader = Loader()
        filepath = self._write_temp_yaml("pi1,alias1,alias2\n")
        with (
            mock.patch.object(invoice_settings, "alias_remote_filepath", filepath),
            mock.patch.object(invoice_settings, "fetch_from_s3", False),
        ):
            result = loader.get_alias_map()
            os.remove(filepath)
            assert result == {"pi1": ["alias1", "alias2"]}

    def test_get_csv_invoice_filepath_list_local(self):
        """When not fetching from S3, list files from the local directory."""
        with tempfile.TemporaryDirectory() as test_dir:
            invoice_file = os.path.join(test_dir, "invoice1.csv")
            with open(invoice_file, "w") as f:
                f.write("data")

            loader = Loader()
            with (
                mock.patch.object(invoice_settings, "fetch_from_s3", False),
                mock.patch.object(invoice_settings, "invoice_path_template", test_dir),
            ):
                result = loader.get_csv_invoice_filepath_list()

        assert result == [invoice_file]

    @mock.patch("process_report.loader.util.get_invoice_bucket")
    def test_get_csv_invoice_filepath_list_s3(self, mock_get_bucket):
        """When fetching from S3, download each service invoice."""
        mock_bucket = mock.MagicMock()
        mock_get_bucket.return_value = mock_bucket

        loader = Loader()
        with (
            mock.patch.object(invoice_settings, "fetch_from_s3", True),
            mock.patch.object(invoice_settings, "invoice_month", "2025-06"),
        ):
            result = loader.get_csv_invoice_filepath_list()

        assert len(result) == 6
        assert mock_bucket.download_file.call_count == 6

    def test_load_dataframe(self):
        """A CSV file is loaded into a DataFrame."""
        filepath = self._write_temp_yaml("a,b\n1,2\n")
        loader = Loader()
        result = loader.load_dataframe(filepath)
        os.remove(filepath)
        assert list(result.columns) == ["a", "b"]
        assert result.iloc[0]["a"] == 1

    def test_load_prepay_credits(self):
        """Prepay credits CSV is loaded and the Credit column is cast."""
        filepath = self._write_temp_yaml("Credit\n100\n")
        loader = Loader()
        with mock.patch.object(invoice_settings, "prepay_credits_filepath", filepath):
            result = loader.load_prepay_credits()
        os.remove(filepath)
        assert result["Credit"].dtype == invoice.BALANCE_FIELD_TYPE
