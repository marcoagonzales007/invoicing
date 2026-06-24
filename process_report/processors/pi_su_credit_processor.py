import logging

from dataclasses import dataclass, field

from process_report.loader import loader
from process_report.invoices import invoice
from process_report.processors import discount_processor


logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


@dataclass
class PISUCreditProcessor(discount_processor.DiscountProcessor):
    """
    This processor operates on data processed by these Processors:
    - ValidateBillablePIsProcessor

    Certain PIs using certain SU types receive a 100% discount on those SUs [1]

    [1] https://github.com/CCI-MOC/invoicing/pull/279#discussion_r3016435299
    """

    IS_DISCOUNT_BY_NERC = True
    PI_SU_CREDIT_CODE = "0005"

    initializes_columns = (
        invoice.CREDIT_COLUMN,
        invoice.CREDIT_CODE_COLUMN,
        invoice.PI_BALANCE_COLUMN,
        invoice.BALANCE_COLUMN,
    )
    operates_on_columns = (
        *initializes_columns,
        invoice.SU_TYPE_COLUMN,
        invoice.PI_COLUMN,
        invoice.COST_COLUMN,
    )

    pi_su_mapping: dict[str, list[str]] = field(
        default_factory=loader.get_pi_non_billed_su_types
    )

    def _process(self):
        for pi, su_types in self.pi_su_mapping.items():
            credit_eligible_rows = self.data[
                self.data[invoice.SU_TYPE_FIELD].isin(su_types)
                & (self.data[invoice.PI_FIELD] == pi)
            ]
            self.apply_flat_discount(
                invoice=self.data,
                pi_projects=credit_eligible_rows,
                pi_balance_field=invoice.PI_BALANCE_FIELD,
                discount_amount=credit_eligible_rows[
                    invoice.COST_FIELD
                ].sum(),  # Discount the entire cost of eligible SUs
                discount_field=invoice.CREDIT_FIELD,
                balance_field=invoice.BALANCE_FIELD,
                code_field=invoice.CREDIT_CODE_FIELD,
                discount_code=self.PI_SU_CREDIT_CODE,
            )
