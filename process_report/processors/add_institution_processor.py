from dataclasses import dataclass
import logging

import pandas

from process_report.invoices import invoice
from process_report.processors import processor
from process_report import util


logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


@dataclass
class AddInstitutionProcessor(processor.Processor):
    operates_on_columns = (
        invoice.INSTITUTION_COLUMN,
        invoice.PI_COLUMN,
        invoice.PROJECT_COLUMN,
    )

    def _add_institution(self):
        """Determine every PI's institution name, logging any PI whose institution cannot be determined
        This is performed by `get_institution_from_pi()`, which tries to match the PI's username to
        a list of known institution email domains (i.e bu.edu), or to several edge cases (i.e rudolph) if
        the username is not an email address.

        Exact matches are then mapped to the corresponding institution name.

        I.e "foo@bu.edu" would match with "bu.edu", which maps to the instition name "Boston University"

        The list of mappings are defined in `institute_map.json`.
        """
        institute_list = util.load_institute_list()
        self.data = self.data.astype({invoice.INSTITUTION_FIELD: "str"})
        for i, row in self.data.iterrows():
            pi_name = row[invoice.PI_FIELD]
            if pandas.isna(pi_name):
                logger.info(f"Project {row[invoice.PROJECT_FIELD]} has no PI")
            else:
                self.data.at[i, invoice.INSTITUTION_FIELD] = (
                    institute_list.get_institution_from_pi(pi_name)
                )

    def _process(self):
        self._add_institution()
