from unittest import TestCase

import pandas

from process_report.invoices import invoice
from process_report.tests import util as test_utils


class TestPISUCreditProcessor(TestCase):
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

    def test_one_eligible_project_only(self):
        """If PI has multiple projects but only one is eligible, only that project should get the credit"""

        invoice_data = self._get_test_invoice(
            pi=["PI", "PI"],
            costs=[50, 75],
            su_type=["Openstack Storage", "HPC"],
        )

        processor = test_utils.new_pi_su_credit_processor(
            invoice_month="2024-06",
            data=invoice_data,
            pi_su_mapping={
                "PI": ["Openstack Storage"]
            },  # Only Openstack Storage SU type is eligible for credit, not HPC
        )
        processor.process()
        output_invoice = processor.data

        expected_invoice = self._get_test_invoice(
            pi=["PI", "PI"],
            costs=[50, 75],
            su_type=["Openstack Storage", "HPC"],
            credit=[50, None],
            credit_code=["0005", None],
            pi_balance=[0, 75],
            balance=[0, 75],
        )

        expected_invoice = expected_invoice.astype(output_invoice.dtypes)
        assert expected_invoice.equals(output_invoice)

    def test_all_eligible_projects(self):
        """PI has multiple projects and all are eligible, so all should get the credit"""
        invoice_data = self._get_test_invoice(
            pi=["PI", "PI"],
            costs=[40, 60],
            su_type=["Openstack Storage", "Openstack Compute"],
        )

        processor = test_utils.new_pi_su_credit_processor(
            invoice_month="2024-06",
            data=invoice_data,
            name="test",
            pi_su_mapping={
                "PI": ["Openstack Storage", "Openstack Compute"]
            },  # Both SU types are eligible for credit
        )
        processor.process()
        output_invoice = processor.data

        expected_invoice = self._get_test_invoice(
            pi=["PI", "PI"],
            costs=[40, 60],
            su_type=["Openstack Storage", "Openstack Compute"],
            credit=[40, 60],
            credit_code=["0005", "0005"],
            pi_balance=[0, 0],
            balance=[0, 0],
        )

        expected_invoice = expected_invoice.astype(output_invoice.dtypes)
        assert expected_invoice.equals(output_invoice)
