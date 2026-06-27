import csv
import pandas

QUOTECHAR = "|"
QUOTING = csv.QUOTE_MINIMAL


def write_invoice_csv(df, filepath):
    df.to_csv(filepath, index=False, quotechar=QUOTECHAR, quoting=QUOTING)


def read_invoice_csv(filepath, dtype=None):
    return pandas.read_csv(filepath, quotechar=QUOTECHAR, dtype=dtype, engine="pyarrow")
