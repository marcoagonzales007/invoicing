import sys
import logging

from dataclasses import dataclass, field
import pandas
import pyarrow

from process_report.settings import invoice_settings
from process_report.loader import loader
from process_report import util
from process_report.invoices import invoice
from process_report.processors import discount_processor


logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


@dataclass
class NewPICreditProcessor(discount_processor.DiscountProcessor):
    """
    This processor operates on data processed by these Processors:
    - ValidateBillablePIsProcessor
    """

    PI_S3_FILEPATH = "PIs/PI.csv"
    PI_S3_BACKUP_FILEPATH = f"PIs/Archive/PI {util.get_iso8601_time()}.csv"
    NEW_PI_CREDIT_CODE = "0002"
    EXCLUDE_SU_TYPES = [
        "OpenShift GPUA100SXM4",
        "OpenStack GPUA100SXM4",
        "OpenShift GPUH100",
        "OpenStack GPUH100",
    ]
    IS_DISCOUNT_BY_NERC = True

    operates_on_columns = (
        invoice.CREDIT_COLUMN,
        invoice.CREDIT_CODE_COLUMN,
        invoice.BALANCE_COLUMN,
        invoice.PI_BALANCE_COLUMN,
        invoice.SU_TYPE_COLUMN,
        invoice.IS_BILLABLE_COLUMN,
        invoice.MISSING_PI_COLUMN,
        invoice.INSTITUTION_COLUMN,
        invoice.COST_COLUMN,
        invoice.PI_COLUMN,
    )

    old_pi_filepath: str = field(
        default_factory=lambda: loader.get_remote_filepath(
            invoice_settings.pi_remote_filepath
        )
    )
    initial_credit_amount: int = field(default_factory=loader.get_new_pi_credit_amount)
    limit_new_pi_credit_to_partners: bool = field(
        default_factory=loader.get_limit_new_pi_credit_to_partners
    )
    upload_to_s3: bool = invoice_settings.upload_to_s3

    @staticmethod
    def _load_old_pis(old_pi_filepath) -> pandas.DataFrame:
        try:
            old_pi_df = pandas.read_csv(
                old_pi_filepath,
                engine="pyarrow",
                dtype={
                    invoice.PI_INITIAL_CREDITS: pandas.ArrowDtype(
                        pyarrow.decimal128(21, 2)
                    ),
                    invoice.PI_1ST_USED: pandas.ArrowDtype(pyarrow.decimal128(21, 2)),
                    invoice.PI_2ND_USED: pandas.ArrowDtype(pyarrow.decimal128(21, 2)),
                },
            )
        except FileNotFoundError:
            sys.exit("Applying credit 0002 failed. Old PI file does not exist")

        return old_pi_df

    @staticmethod
    def _get_pi_age(old_pi_df: pandas.DataFrame, pi, invoice_month):
        """Returns time difference between current invoice month and PI's first invoice month
        I.e 0 for new PIs
        Will raise an error if the PI'a age is negative, which suggests a faulty invoice, or a program bug"""
        first_invoice_month = old_pi_df.loc[
            old_pi_df[invoice.PI_PI_FIELD] == pi, invoice.PI_FIRST_MONTH
        ]
        if first_invoice_month.empty:
            return 0

        month_diff = util.get_month_diff(invoice_month, first_invoice_month.iat[0])
        if month_diff < 0:
            sys.exit(
                f"PI {pi} from {first_invoice_month} found in {invoice_month} invoice!"
            )
        else:
            return month_diff

    @staticmethod
    def _upsert_pi_entry(
        old_pi_df,
        pi_name,
        invoice_month,
        initial_credits,
        first_month_used,
        second_month_used,
    ) -> pandas.DataFrame:
        """
        Upserts PI entry in old PI dataframe.

        If the PI already has an entry, overwrites the entry with new values, otherwise creates a new entry.

        This returns the updated old_pi_df
        """
        pi_entry = [
            pi_name,
            invoice_month,
            initial_credits,
            first_month_used,
            second_month_used,
        ]
        if old_pi_df.loc[old_pi_df[invoice.PI_PI_FIELD] == pi_name].empty:
            old_pi_df = pandas.concat(
                [
                    pandas.DataFrame([pi_entry], columns=old_pi_df.columns),
                    old_pi_df,
                ],
                ignore_index=True,
            )
        else:
            old_pi_df.loc[old_pi_df[invoice.PI_PI_FIELD] == pi_name] = pi_entry

        return old_pi_df

    def _filter_partners(self, data):
        active_partnerships = list()
        institute_list = util.load_institute_list()
        for institute_info in institute_list.root:
            if partnership_start_date := institute_info.mghpcc_partnership_start_date:
                if util.get_month_diff(self.invoice_month, partnership_start_date) >= 0:
                    active_partnerships.append(institute_info.display_name)

        return data[data[invoice.INSTITUTION_FIELD].isin(active_partnerships)]

    def _filter_excluded_su_types(self, data):
        return data[~(data[invoice.SU_TYPE_FIELD].isin(self.EXCLUDE_SU_TYPES))]

    def _filter_nonbillables(self, data):
        return data[data["Is Billable"]]

    def _filter_missing_pis(self, data):
        return data[~data["Missing PI"]]

    def _get_credit_eligible_projects(self, data: pandas.DataFrame):
        filtered_data = self._filter_nonbillables(data)
        filtered_data = self._filter_missing_pis(filtered_data)
        filtered_data = self._filter_excluded_su_types(filtered_data)
        if self.limit_new_pi_credit_to_partners:
            filtered_data = self._filter_partners(filtered_data)

        return filtered_data

    def _apply_credits_new_pi(
        self, data: pandas.DataFrame, old_pi_df: pandas.DataFrame
    ):
        logger.info(
            f"New PI Credit set at {self.initial_credit_amount} for {self.invoice_month}"
        )

        credit_eligible_projects = self._get_credit_eligible_projects(data)
        all_pi_set = set(data[~data[invoice.MISSING_PI_FIELD]][invoice.PI_FIELD])
        eligible_pi_set = set(credit_eligible_projects[invoice.PI_FIELD])
        for pi in all_pi_set:
            pi_age = self._get_pi_age(old_pi_df, pi, self.invoice_month)
            pi_old_pi_entry = old_pi_df.loc[
                old_pi_df[invoice.PI_PI_FIELD] == pi
            ].squeeze()

            # If the pi is not eligible, their initial credit amount is set to 0,
            # but we still want to keep track of them in case they become eligible in the future
            # More detail: https://github.com/CCI-MOC/invoicing/issues/280
            if pi not in eligible_pi_set and pi_age == 0:
                old_pi_df = self._upsert_pi_entry(
                    old_pi_df, pi, self.invoice_month, 0, 0, 0
                )
                continue

            pi_projects = credit_eligible_projects[
                credit_eligible_projects[invoice.PI_FIELD] == pi
            ]

            if pi_age <= 1:
                if pi_age == 0:
                    old_pi_df = self._upsert_pi_entry(
                        old_pi_df,
                        pi,
                        self.invoice_month,
                        self.initial_credit_amount,
                        0,
                        0,
                    )
                    pi_old_pi_entry = old_pi_df.loc[
                        old_pi_df[invoice.PI_PI_FIELD] == pi
                    ].squeeze()

                    remaining_credit = self.initial_credit_amount
                    credit_used_field = invoice.PI_1ST_USED
                elif pi_age == 1:
                    remaining_credit = (
                        pi_old_pi_entry[invoice.PI_INITIAL_CREDITS]
                        - pi_old_pi_entry[invoice.PI_1ST_USED]
                    )
                    credit_used_field = invoice.PI_2ND_USED

                credits_used = self.apply_flat_discount(
                    data,
                    pi_projects,
                    invoice.PI_BALANCE_FIELD,
                    remaining_credit,
                    invoice.CREDIT_FIELD,
                    invoice.BALANCE_FIELD,
                    invoice.CREDIT_CODE_FIELD,
                    self.NEW_PI_CREDIT_CODE,
                )

                if (pi_old_pi_entry[credit_used_field] != 0) and (
                    credits_used != pi_old_pi_entry[credit_used_field]
                ):
                    logger.warning(
                        f"PI file overwritten. PI {pi} previously used ${pi_old_pi_entry[credit_used_field]} of New PI credits, now uses ${credits_used}"
                    )
                old_pi_df.loc[
                    old_pi_df[invoice.PI_PI_FIELD] == pi, credit_used_field
                ] = credits_used

        return (data, old_pi_df)

    def _prepare(self):
        self.old_pi_df = self._load_old_pis(self.old_pi_filepath)

    def _process(self):
        self.data, self.updated_old_pi_df = self._apply_credits_new_pi(
            self.data, self.old_pi_df
        )
        self._export_updated_old_pi_file()
        if self.upload_to_s3:
            self.s3_bucket = util.get_invoice_bucket()
            self._export_s3_updated_old_pi_file()
            self._backup_s3_updated_old_pi_file()

    def _export_updated_old_pi_file(self):
        self.updated_old_pi_df.to_csv(self.old_pi_filepath, index=False)

    def _export_s3_updated_old_pi_file(self):
        self.s3_bucket.upload_file(self.old_pi_filepath, self.PI_S3_FILEPATH)

    def _backup_s3_updated_old_pi_file(self):
        self.s3_bucket.upload_file(self.old_pi_filepath, self.PI_S3_BACKUP_FILEPATH)
