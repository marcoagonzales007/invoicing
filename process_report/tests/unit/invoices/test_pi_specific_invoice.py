import tempfile
from unittest import TestCase, mock
import pandas

from process_report.tests import util as test_utils
from process_report.invoices.pi_specific_invoice import CHROME_BIN_PATH


class TestPISpecificInvoice(TestCase):
    def _get_test_invoice(
        self,
        pi,
        institution,
        balance,
        is_billable=None,
        missing_pi=None,
        group_name=None,
    ):
        if not is_billable:
            is_billable = [True] * len(pi)

        if not missing_pi:
            missing_pi = [False] * len(pi)

        if not group_name:
            group_name = [None] * len(pi)

        return pandas.DataFrame(
            {
                "Manager (PI)": pi,
                "Institution": institution,
                "Is Billable": is_billable,
                "Missing PI": missing_pi,
                "Prepaid Group Name": group_name,
                "Prepaid Group Institution": ["" for _ in range(len(pi))],
                "Prepaid Group Balance": [0 for _ in range(len(pi))],
                "Prepaid Group Used": [0 for _ in range(len(pi))],
                "Balance": balance,
            }
        )

    def test_get_pi_dataframe(self):
        def add_dollar_sign(data):
            if pandas.isna(data):
                return data
            else:
                return "$" + str(data)

        test_invoice = self._get_test_invoice(
            ["PI1", "PI1", "PI2", "PI2"],
            [
                "BU",
                "BU",
                "HU",
                "HU",
            ],
            [100, 200, 300, 400],
            group_name=[None, "G1", None, None],
        )
        answer_invoice_pi1 = (
            test_invoice[test_invoice["Manager (PI)"] == "PI1"]
            .copy()
            .reset_index(drop=True)
        )
        # Create totals row by copying first row and modifying
        totals_row = answer_invoice_pi1.iloc[[0]].copy()
        for col in totals_row.columns:
            totals_row[col] = ""
        totals_row["Invoice Month"] = "Total"
        totals_row["Balance"] = 300
        answer_invoice_pi1 = pandas.concat(
            [answer_invoice_pi1, totals_row], ignore_index=True
        )

        # Apply dollar formatting
        for column_name in [
            "Prepaid Group Balance",
            "Prepaid Group Used",
            "Balance",
        ]:
            answer_invoice_pi1[column_name] = answer_invoice_pi1[column_name].apply(
                add_dollar_sign
            )
        answer_invoice_pi1 = answer_invoice_pi1.astype(pandas.StringDtype())
        answer_invoice_pi1.fillna("", inplace=True)

        answer_invoice_pi2 = (
            test_invoice[test_invoice["Manager (PI)"] == "PI2"]
            .copy()
            .reset_index(drop=True)
        )

        # Create totals row by copying first row and modifying to preserve formatting
        totals_row = answer_invoice_pi2.iloc[[0]].copy()
        for col in totals_row.columns:
            totals_row[col] = ""
        totals_row["Invoice Month"] = "Total"
        totals_row["Balance"] = 700
        answer_invoice_pi2 = pandas.concat(
            [answer_invoice_pi2, totals_row], ignore_index=True
        )

        # Drop prepay columns (they're all NA for PI2)
        answer_invoice_pi2 = answer_invoice_pi2.drop(
            [
                "Prepaid Group Name",
                "Prepaid Group Institution",
                "Prepaid Group Balance",
                "Prepaid Group Used",
            ],
            axis=1,
        )
        answer_invoice_pi2["Balance"] = answer_invoice_pi2["Balance"].apply(
            add_dollar_sign
        )
        answer_invoice_pi2 = answer_invoice_pi2.astype(pandas.StringDtype())
        answer_invoice_pi2.fillna("", inplace=True)

        pi_inv = test_utils.new_pi_specific_invoice(data=test_invoice)
        output_invoice = pi_inv._get_pi_dataframe(test_invoice, "PI1")
        assert answer_invoice_pi1.equals(output_invoice)

        output_invoice = pi_inv._get_pi_dataframe(test_invoice, "PI2")
        assert answer_invoice_pi2.equals(output_invoice)

    @mock.patch("process_report.invoices.invoice.Invoice._filter_columns")
    @mock.patch("os.path.exists")
    @mock.patch("subprocess.run")
    def test_export_pi(self, mock_subprocess_run, mock_path_exists, mock_filter_cols):
        invoice_month = "2024-10"
        test_invoice = self._get_test_invoice(
            ["PI1", "PI1", "PI2", "PI2"],
            [
                "BU",
                "BU",
                "HU",
                "HU",
            ],
            [100, 200, 300, 400],
            group_name=[None, "G1", None, None],
        )

        mock_filter_cols.return_value = test_invoice
        mock_path_exists.return_value = True

        with tempfile.TemporaryDirectory() as test_dir:
            pi_inv = test_utils.new_pi_specific_invoice(
                test_dir, invoice_month, data=test_invoice
            )
            pi_inv.process()
            pi_inv.export()
            pi_pdf_1 = f"{test_dir}/BU_PI1_{invoice_month}.pdf"
            pi_pdf_2 = f"{test_dir}/HU_PI2_{invoice_month}.pdf"

            for i, pi_pdf_path in enumerate([pi_pdf_1, pi_pdf_2]):
                chrome_arglist, _ = mock_subprocess_run.call_args_list[i]
                answer_arglist = [
                    CHROME_BIN_PATH,
                    "--headless",
                    "--no-sandbox",
                    f"--print-to-pdf={pi_pdf_path}",
                    "--no-pdf-header-footer",
                ]

                self.assertEqual(answer_arglist, chrome_arglist[0][:-1])

    @mock.patch("process_report.invoices.invoice.Invoice._filter_columns")
    @mock.patch("os.path.exists")
    @mock.patch("subprocess.run")
    def test_process_no_warnings(
        self, mock_subprocess_run, mock_path_exists, mock_filter_cols
    ):
        """Test that no warnings are raised during invoice processing"""
        invoice_month = "2024-10"
        test_invoice = self._get_test_invoice(
            ["PI1", "PI1", "PI2", "PI2"],
            ["BU", "BU", "HU", "HU"],
            [100, 200, 300, 400],
            group_name=[None, "G1", None, None],
        )
        with tempfile.TemporaryDirectory() as test_dir:
            pi_inv = test_utils.new_pi_specific_invoice(
                test_dir, invoice_month, data=test_invoice
            )
            with self.assertNoLogs(
                "process_report.invoices.pi_specific_invoice", level="WARNING"
            ):
                pi_inv.process()
                pi_inv.export()

    @mock.patch("process_report.invoices.invoice.Invoice._filter_columns")
    @mock.patch("os.path.exists")
    @mock.patch("subprocess.run")
    def test_export_missing_pi(
        self, mock_subprocess_run, mock_path_exists, mock_filter_cols
    ):
        invoice_month = "2025-01"
        test_invoice = self._get_test_invoice(
            # First row has no PI and so should be skipped during export
            pi=[None, "PI1"],
            institution=["", "BU"],
            balance=[0, 100],
        )
        mock_path_exists.return_value = True
        mock_filter_cols.return_value = test_invoice
        with tempfile.TemporaryDirectory() as test_dir:
            pi_inv = test_utils.new_pi_specific_invoice(
                test_dir, invoice_month, data=test_invoice
            )
            pi_inv.process()
            pi_inv.export()
            assert mock_subprocess_run.call_count == 1

    @mock.patch("process_report.invoices.invoice.Invoice._filter_columns")
    @mock.patch("os.path.exists")
    def test_export_no_chrome(self, mock_path_exists, mock_filter_cols):
        invoice_month = "2025-01"
        test_invoice = self._get_test_invoice(
            pi=[None, "PI1"],
            institution=["", "BU"],
            balance=[0, 100],
        )
        mock_path_exists.return_value = False
        mock_filter_cols.return_value = test_invoice
        with tempfile.TemporaryDirectory() as test_dir:
            pi_inv = test_utils.new_pi_specific_invoice(
                test_dir, invoice_month, data=test_invoice
            )
            pi_inv.process()
            with self.assertRaises(SystemExit):
                pi_inv.export()

    @mock.patch("process_report.util.get_iso8601_time")
    @mock.patch("os.listdir")
    def test_export_s3(self, mock_listdir, mock_get_time):
        mock_get_time.return_value = "2025-01-01"
        mock_listdir.return_value = ["BU_PI1_2025-01.pdf"]
        s3_bucket = mock.MagicMock()
        invoice_month = "2025-01"

        pi_inv = test_utils.new_pi_specific_invoice("test_dir", invoice_month)
        pi_inv.export_s3(s3_bucket)
        expected_path = "test_dir/BU_PI1_2025-01.pdf"
        expected_s3_archive_path = (
            "Invoices/2025-01/Archive/test_dir/BU_PI1_2025-01 2025-01-01.pdf"
        )
        expected_s3_path = "Invoices/2025-01/test_dir/BU_PI1_2025-01.pdf"
        s3_bucket.upload_file.assert_any_call(expected_path, expected_s3_archive_path)
        s3_bucket.upload_file.assert_any_call(expected_path, expected_s3_path)
        assert s3_bucket.upload_file.call_count == 2
