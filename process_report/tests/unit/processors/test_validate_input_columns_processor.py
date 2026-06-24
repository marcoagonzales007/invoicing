import pandas

from process_report.processors.validate_input_column_processor import (
    ValidateInputColumnsProcessor,
)
from process_report.tests.base import BaseTestCase


class TestValidateInputColumnsProcessor(BaseTestCase):
    def test_process_succeeds_when_required_columns_exist_and_keeps_extra_columns(self):
        invoice_month = "2025-01"
        test_data_dict = {
            "Invoice Month": [invoice_month],
            "Project - Allocation": ["P1"],
            "Project - Allocation ID": ["P1-ID"],
            "Manager (PI)": ["pi1"],
            "Cluster Name": ["cluster1"],
            "Invoice Email": ["pi1@example.com"],
            "Invoice Address": ["123 Main St"],
            "Institution": ["Example University"],
            "Institution - Specific Code": ["EX-001"],
            "SU Hours (GBhr or SUhr)": [10],
            "SU Type": ["Compute"],
            "Rate": ["1.00"],
            "Cost": [100.0],
            "Extra Column": ["extra"],
        }

        processor = ValidateInputColumnsProcessor(
            invoice_month=invoice_month, data=pandas.DataFrame(test_data_dict)
        )
        processor.process()

        output_data = processor.data
        expected_data = self.create_test_invoice(test_data_dict)
        assert output_data.equals(expected_data)

    def test_process_raises_error_when_required_columns_are_missing(self):
        invoice_month = "2025-01"
        test_data = pandas.DataFrame(
            {"Invoice Month": [invoice_month], "Cost": [100.0]}
        )

        processor = ValidateInputColumnsProcessor(
            invoice_month=invoice_month, data=test_data
        )

        expected_message = """Input dataframe is missing required columns: Project - Allocation, Project - Allocation ID, Manager (PI),
            Cluster Name, Invoice Email, Invoice Address, Institution, Institution - Specific Code, SU Hours (GBhr or SUhr),
            SU Type, Rate. Stopping invoicing"""

        with self.assertRaises(ValueError, msg=expected_message):
            processor.process()
