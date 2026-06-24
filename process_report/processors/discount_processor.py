import pandas

from process_report.processors import processor


class DiscountProcessor(processor.Processor):
    """
    Processor class containing functions useful for applying discounts
    on dataframes
    """

    IS_DISCOUNT_BY_NERC = True

    def apply_flat_discount(
        self,
        invoice: pandas.DataFrame,
        pi_projects: pandas.DataFrame,
        pi_balance_field: str,
        discount_amount: float,
        discount_field: str,
        balance_field: str,
        code_field: str = None,
        discount_code: str = None,
    ):
        """
        Takes in an invoice and a list of PI projects that are a subset of it,
        and applies a flat discount to those PI projects. Note that this function
        will change the provided `invoice` Dataframe directly. Therefore, it does
        not return the changed invoice.

        This function assumes that the balance field shows the remaining cost of the project,
        or what the PI would pay before the flat discount is applied.

        If the optional parameters `code_field` and `discount_code` are passed in,
        `discount_code` will be comma-APPENDED to the `code_field` of projects where
        the discount is applied

        Returns the amount of discount used.

        :param invoice: Dataframe containing all projects
        :param pi_projects: A subset of `invoice`, containing all projects for a PI you want to apply the discount
        :param pi_balance_field: Name of the field of the PI balance
        :param discount_amount: The discount given to the PI
        :param discount_field: Name of the field to put the discount amount applied to each project
        :param balance_field: Name of the NERC balance field
        :param code_field: Name of the discount code field
        :param discount_code: Code of the discount
        """

        def apply_discount_on_project(remaining_discount_amount, project_i, project):
            remaining_project_balance = project[pi_balance_field]
            applied_discount = min(remaining_project_balance, remaining_discount_amount)

            if pandas.isna(invoice.at[project_i, discount_field]):
                invoice.at[project_i, discount_field] = applied_discount
            else:
                invoice.at[project_i, discount_field] += applied_discount

            invoice.at[project_i, pi_balance_field] -= applied_discount
            if self.IS_DISCOUNT_BY_NERC:
                invoice.at[project_i, balance_field] -= applied_discount
            remaining_discount_amount -= applied_discount
            return remaining_discount_amount

        def apply_credit_code_on_project(project_i):
            if code_field and discount_code:
                if pandas.isna(invoice.at[project_i, code_field]):
                    invoice.at[project_i, code_field] = discount_code
                else:
                    invoice.at[project_i, code_field] = (
                        invoice.at[project_i, code_field] + "," + discount_code
                    )

        remaining_discount_amount = discount_amount
        for i, row in pi_projects.iterrows():
            if remaining_discount_amount == 0:
                break
            else:
                remaining_discount_amount = apply_discount_on_project(
                    remaining_discount_amount, i, row
                )
                apply_credit_code_on_project(i)

        discount_used = discount_amount - remaining_discount_amount
        return discount_used
