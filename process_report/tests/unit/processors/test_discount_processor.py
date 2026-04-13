from unittest import TestCase

import pandas

from process_report.invoices import invoice
from process_report.processors.discount_processor import DiscountProcessor


class TestDiscountProcessor(TestCase):
    def _get_test_invoice(
        self,
        pi,
        costs,
        su_type,
        credit=None,
        credit_code=None,
        pi_balance=None,
        balance=None,
    ):
        if credit is None:
            credit = [None for _ in range(len(pi))]
        if credit_code is None:
            credit_code = [None for _ in range(len(pi))]
        if pi_balance is None:
            pi_balance = costs
        if balance is None:
            balance = costs

        return pandas.DataFrame(
            {
                invoice.PI_FIELD: pi,
                invoice.SU_TYPE_FIELD: su_type,
                invoice.COST_FIELD: costs,
                invoice.CREDIT_FIELD: credit,
                invoice.CREDIT_CODE_FIELD: credit_code,
                invoice.PI_BALANCE_FIELD: pi_balance,
                invoice.BALANCE_FIELD: balance,
            }
        )

    def test_preexisting_credit(
        self,
    ):
        """Tests that if there is already a credit in the invoice, the discount is added to it rather than overwriting it"""
        invoice_data = self._get_test_invoice(
            pi=["PI"],
            costs=[100],
            su_type=["Openstack Storage"],
            credit=[10],
            credit_code=["0003"],
            pi_balance=[90],
            balance=[90],
        )

        processor = DiscountProcessor(
            invoice_month="2024-06",
            data=invoice_data,
            name="test",
        )
        processor.apply_flat_discount(
            invoice=invoice_data,
            pi_projects=invoice_data,
            pi_balance_field=invoice.PI_BALANCE_FIELD,
            discount_amount=invoice_data[invoice.COST_FIELD].sum(),
            discount_field=invoice.CREDIT_FIELD,
            balance_field=invoice.BALANCE_FIELD,
            code_field=invoice.CREDIT_CODE_FIELD,
            discount_code="0005",
        )
        output_invoice = processor.data

        expected_invoice = self._get_test_invoice(
            pi=["PI"],
            costs=[100],
            su_type=["Openstack Storage"],
            credit=[100],
            credit_code=["0003,0005"],
            pi_balance=[0],
            balance=[0],
        )

        assert expected_invoice.equals(output_invoice)
