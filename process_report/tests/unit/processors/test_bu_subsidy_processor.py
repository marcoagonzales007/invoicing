from process_report.tests import util as test_utils
from process_report.tests.base import BaseTestCase


class TestBUSubsidyProcessor(BaseTestCase):
    def _assert_result_invoice(
        self,
        subsidy_amount,
        test_invoice,
        answer_invoice,
        invoice_month="0000-00",
    ):
        new_bu_subsidy_proc = test_utils.new_bu_subsidy_processor(
            invoice_month=invoice_month,
            data=test_invoice,
            subsidy_amount=subsidy_amount,
        )
        new_bu_subsidy_proc.process()
        output_invoice = new_bu_subsidy_proc.data
        answer_invoice = answer_invoice.astype(output_invoice.dtypes)

        assert output_invoice.equals(answer_invoice)

    def _get_test_invoice(
        self,
        pi,
        pi_balances,
        balances=None,
        project_names=None,
        institution=None,
        is_billable=None,
        missing_pi=None,
    ):
        if not balances:
            balances = pi_balances

        if not project_names:
            project_names = ["Project" for _ in range(len(pi))]

        if not institution:
            institution = ["Boston University" for _ in range(len(pi))]

        if not is_billable:
            is_billable = [True for _ in range(len(pi))]

        if not missing_pi:
            missing_pi = [False for _ in range(len(pi))]

        return self.create_test_invoice(
            {
                "Manager (PI)": pi,
                "Project - Allocation": project_names,
                "PI Balance": pi_balances,
                "Balance": balances,
                "Institution": institution,
                "Is Billable": is_billable,
                "Missing PI": missing_pi,
                "Subsidy": [0 for _ in range(len(pi))],
            }
        )

    def test_exclude_non_BU_pi(self):
        """Are only BU PIs given the subsidy?"""

        subsidy_amount = 100
        test_invoice = self._get_test_invoice(
            [str(i) for i in range(5)],
            pi_balances=[subsidy_amount for _ in range(5)],
            institution=[
                "Boston University",
                "Boston University",
                "boston university",
                "Harvard University",
                "BU",
            ],
        )

        answer_invoice = test_invoice.copy()
        answer_invoice["Project"] = answer_invoice["Project - Allocation"]
        answer_invoice["Subsidy"] = [subsidy_amount, subsidy_amount, 0, 0, 0]
        answer_invoice["PI Balance"] = [
            0,
            0,
            subsidy_amount,
            subsidy_amount,
            subsidy_amount,
        ]

        self._assert_result_invoice(subsidy_amount, test_invoice, answer_invoice)

    def test_exclude_nonbillables(self):
        """Are nonbillables excluded from the subsidy?"""
        subsidy_amount = 100
        test_invoice = self._get_test_invoice(
            [str(i) for i in range(6)],
            pi_balances=[subsidy_amount for _ in range(6)],
            is_billable=[True, True, False, False, True, True],
            missing_pi=[True, True, False, False, False, False],
        )

        answer_invoice = test_invoice.copy()
        answer_invoice["Project"] = answer_invoice["Project - Allocation"]
        answer_invoice["Subsidy"] = [0, 0, 0, 0, subsidy_amount, subsidy_amount]
        answer_invoice["PI Balance"] = [
            subsidy_amount,
            subsidy_amount,
            subsidy_amount,
            subsidy_amount,
            0,
            0,
        ]

        self._assert_result_invoice(subsidy_amount, test_invoice, answer_invoice)

    def test_one_pi_many_allocations(self):
        """Is subsidy applied properly to BU PI with many allocations?"""

        # Two projects, one allocation each
        subsidy_amount = 100
        test_invoice = self._get_test_invoice(
            ["PI" for i in range(2)],
            pi_balances=[60, 60],
            project_names=["P1", "P2"],
        )

        answer_invoice = test_invoice.copy()
        answer_invoice["Project"] = answer_invoice["Project - Allocation"]
        answer_invoice["Subsidy"] = [60, 40]
        answer_invoice["PI Balance"] = [0, 20]

        self._assert_result_invoice(subsidy_amount, test_invoice, answer_invoice)

        # Two projects, two allocations each
        test_invoice = self._get_test_invoice(
            ["PI" for i in range(4)],
            pi_balances=[40, 40, 40, 40],
            project_names=["P1-A1", "P1-A1-test", "P2", "P2-"],
        )

        answer_invoice = test_invoice.copy()
        answer_invoice["Project"] = ["P1", "P1-A1", "P2", "P2"]
        answer_invoice["Subsidy"] = [40, 40, 20, 0]
        answer_invoice["PI Balance"] = [0, 0, 20, 40]

        self._assert_result_invoice(subsidy_amount, test_invoice, answer_invoice)

        # Two allocations, one where PI balance != NERC balance
        test_invoice = self._get_test_invoice(
            ["PI" for i in range(2)],
            pi_balances=[80, 80],
            project_names=["P1", "P2"],
            balances=[100, 80],
        )

        answer_invoice = test_invoice.copy()
        answer_invoice["Project"] = answer_invoice["Project - Allocation"]
        answer_invoice["Subsidy"] = [80, 20]
        answer_invoice["PI Balance"] = [0, 60]

        self._assert_result_invoice(subsidy_amount, test_invoice, answer_invoice)

    def test_two_pi(self):
        """Is subsidy applied to more than one PI?"""
        # Each PI has two allocations
        subsidy_amount = 100
        test_invoice = self._get_test_invoice(
            ["PI1", "PI1", "PI2", "PI2"],
            pi_balances=[80, 80, 40, 40],
        )

        answer_invoice = test_invoice.copy()
        answer_invoice["Project"] = answer_invoice["Project - Allocation"]
        answer_invoice["Subsidy"] = [80, 20, 40, 40]
        answer_invoice["PI Balance"] = [0, 60, 0, 0]

        self._assert_result_invoice(subsidy_amount, test_invoice, answer_invoice)
