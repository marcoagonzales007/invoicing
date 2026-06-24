from dataclasses import dataclass

from process_report.invoices import invoice
from process_report.processors import processor


@dataclass
class ValidateInputColumnsProcessor(processor.Processor):
    initializes_columns = (
        invoice.INVOICE_DATE_COLUMN,
        invoice.PROJECT_COLUMN,
        invoice.PROJECT_ID_COLUMN,
        invoice.PI_COLUMN,
        invoice.CLUSTER_NAME_COLUMN,
        invoice.INVOICE_EMAIL_COLUMN,
        invoice.INVOICE_ADDRESS_COLUMN,
        invoice.INSTITUTION_COLUMN,
        invoice.INSTITUTION_ID_COLUMN,
        invoice.SU_HOURS_COLUMN,
        invoice.SU_TYPE_COLUMN,
        invoice.RATE_COLUMN,
        invoice.COST_COLUMN,
    )

    def process(self):
        missing_columns = [
            column.name
            for column in self.initializes_columns
            if column.name not in self.data.columns
        ]
        if missing_columns:
            raise ValueError(
                f"Input dataframe is missing required columns: {', '.join(missing_columns)}. Stopping invoicing"
            )

        # Casts columns to appropriate types
        self._init_columns()
