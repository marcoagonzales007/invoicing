from unittest import TestCase, mock
import pandas

from process_report.tests import util as test_utils


class TestCreditsSnapshot(TestCase):
    def _get_test_prepay_credits(self, months, group_names, credits):
        return pandas.DataFrame(
            {"Month": months, "Group Name": group_names, "Credit": credits}
        )

    def _get_test_prepay_contacts(self, group_names, emails, is_managed):
        return pandas.DataFrame(
            {
                "Group Name": group_names,
                "Group Contact Email": emails,
                "MGHPCC Managed": is_managed,
            }
        )

    def test_get_credit_snapshot(self):
        invoice_month = "2024-10"
        test_prepay_credits = self._get_test_prepay_credits(
            ["2024-10", "2024-10", "2024-10", "2024-09", "2024-09"],
            ["G1", "G2", "G3", "G1", "G2"],
            [0] * 5,
        )
        test_prepay_contacts = self._get_test_prepay_contacts(
            ["G1", "G2", "G3"], [""] * 3, ["Yes", "No", "Yes"]
        )
        answer_credits_snapshot = test_prepay_credits.iloc[[0, 2]]

        new_prepayment_proc = test_utils.new_prepay_credits_snapshot(
            invoice_month=invoice_month,
            prepay_credits=test_prepay_credits,
            prepay_contacts=test_prepay_contacts,
        )
        new_prepayment_proc._prepare()
        assert answer_credits_snapshot.equals(new_prepayment_proc.export_data)

    def test_output_path(self):
        inv = test_utils.new_prepay_credits_snapshot(invoice_month="2025-01")

        assert inv.output_path == "NERC_Prepaid_Group-Credits-2025-01.csv"

    def test_output_s3_key(self):
        inv = test_utils.new_prepay_credits_snapshot(invoice_month="2025-01")
        assert (
            inv.output_s3_key
            == "Invoices/2025-01/NERC_Prepaid_Group-Credits-2025-01.csv"
        )

    @mock.patch("process_report.util.get_iso8601_time")
    def test_output_s3_archive_key(self, mock_time):
        mock_time.return_value = "2025-01-01T00:00:00"
        inv = test_utils.new_prepay_credits_snapshot(invoice_month="2025-01")
        assert (
            inv.output_s3_archive_key
            == "Invoices/2025-01/Archive/NERC_Prepaid_Group-Credits-2025-01 2025-01-01T00:00:00.csv"
        )
