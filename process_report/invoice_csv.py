import csv
import pandas

QUOTECHAR = "|"
QUOTING = csv.QUOTE_MINIMAL


def _invoice_dtypes():
    from process_report.invoices import invoice

    return {
        col.name: col.dtype
        for col in [
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
        ]
    }


def write_invoice_csv(df, filepath):
    df.to_csv(filepath, index=False, quotechar=QUOTECHAR, quoting=QUOTING)


def read_invoice_csv(filepath):
    return pandas.read_csv(
        filepath, quotechar=QUOTECHAR, dtype=_invoice_dtypes(), engine="pyarrow"
    )
