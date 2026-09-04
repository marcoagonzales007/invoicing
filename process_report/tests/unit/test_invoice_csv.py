import tempfile
from unittest import TestCase
from process_report.invoices import invoice
import pandas

from process_report import invoice_csv


class TestInvoiceCSV(TestCase):
    def _round_trip(self, df):
        with tempfile.NamedTemporaryFile(suffix=".csv") as f:
            invoice_csv.write_invoice_csv(df, f.name)
            return invoice_csv.read_invoice_csv(f.name)

    def test_round_trip_simple(self):
        test_invoice = pandas.DataFrame({"C1": ["proj1", "proj2"], "C2": ["a", "b"]})
        result_invoice = self._round_trip(test_invoice)

        assert result_invoice.equals(test_invoice)

    def test_round_trip_field_with_comma(self):
        test_invoice = pandas.DataFrame({"C1": ["Green, John"], "C2": ["Y"]})
        result_invoice = self._round_trip(test_invoice)

        assert result_invoice.equals(test_invoice)

    def test_reader_casts_invoice_columns(self):
        test_invoice = pandas.DataFrame({invoice.COST_FIELD: [100]})
        result_invoice = self._round_trip(test_invoice)

        assert result_invoice[invoice.COST_FIELD].dtype == invoice.BALANCE_FIELD_TYPE

    def test_written_file_uses_standard_quotechar(self):
        test_invoice = pandas.DataFrame({"Manager (PI)": ["Green, John"]})
        with tempfile.NamedTemporaryFile(suffix=".csv", mode="r+") as f:
            invoice_csv.write_invoice_csv(test_invoice, f.name)
            contents = f.read()

        assert "|Green, John|" in contents
