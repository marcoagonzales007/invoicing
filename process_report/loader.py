from decimal import Decimal
import datetime
import functools
import os
import yaml

import pandas
from nerc_rates import load_from_url

from process_report import util
from process_report.settings import invoice_settings
from process_report.invoices import invoice
from process_report.nonbillable_models import ExcludedProjectList, PIList

# List of service invoices processed by pipeline. Change if new services are added.
# Cannot simply filter by suffix because S3 can't do it
S3_SERVICE_INVOICE_LIST = [
    "ocp-test {invoice_month}.csv",
    "ocp-prod {invoice_month}.csv",
    "academic {invoice_month}.csv",
    "barcelona {invoice_month}.csv",
    "NERC OpenStack {invoice_month}.csv",
    "NERC Storage {invoice_month}.csv",
]


@functools.lru_cache
def get_rates_info():
    return load_from_url()


class Loader:
    @functools.lru_cache
    def get_csv_invoice_filepath_list(self) -> list[str]:
        """Fetch invoice CSV files from S3 if fetch_from_s3 is True. Returns local paths of files."""
        csv_invoice_filepath_list = []
        if invoice_settings.fetch_from_s3:
            s3_bucket = util.get_invoice_bucket()

            for invoice_name_template in S3_SERVICE_INVOICE_LIST:
                local_name = invoice_name_template.format(
                    invoice_month=invoice_settings.invoice_month
                )
                s3_name = (invoice_settings.invoice_path_template + local_name).format(
                    invoice_month=invoice_settings.invoice_month
                )
                csv_invoice_filepath_list.append(local_name)
                s3_bucket.download_file(s3_name, local_name)
        else:
            invoice_dir_path = invoice_settings.invoice_path_template.format(
                invoice_month=invoice_settings.invoice_month
            )
            for invoice in os.listdir(invoice_dir_path):
                invoice_absolute_path = os.path.join(invoice_dir_path, invoice)
                csv_invoice_filepath_list.append(invoice_absolute_path)

        return csv_invoice_filepath_list

    @functools.lru_cache
    def get_remote_filepath(self, remote_filepath: str) -> str:
        """Fetch a file from S3 if fetch_from_s3 is True. Returns local path of file."""
        if invoice_settings.fetch_from_s3:
            return util.fetch_s3(remote_filepath)
        return remote_filepath

    @functools.lru_cache
    def get_new_pi_credit_amount(self) -> Decimal:
        return invoice_settings.new_pi_credit_amount or get_rates_info().get_value_at(
            "New PI Credit", invoice_settings.invoice_month, Decimal
        )

    @functools.lru_cache
    def get_limit_new_pi_credit_to_partners(self) -> bool:
        return (
            invoice_settings.limit_new_pi_credit_to_partners
            or get_rates_info().get_value_at(
                "Limit New PI Credit to MGHPCC Partners",
                invoice_settings.invoice_month,
                bool,
            )
        )

    @functools.lru_cache
    def get_bu_subsidy_amount(self) -> Decimal:
        return invoice_settings.bu_subsidy_amount or get_rates_info().get_value_at(
            "BU Subsidy", invoice_settings.invoice_month, Decimal
        )

    @functools.lru_cache
    def get_lenovo_su_charge_info(self) -> dict[str, Decimal]:
        if invoice_settings.lenovo_charge_info:
            return invoice_settings.lenovo_charge_info

        lenovo_charge_info = {}
        for su_name in ["GPUA100SXM4", "GPUH100"]:
            lenovo_charge_info[su_name] = get_rates_info().get_value_at(
                f"Lenovo {su_name} Charge", invoice_settings.invoice_month, Decimal
            )
        return lenovo_charge_info

    @functools.lru_cache
    def get_alias_map(self) -> dict:
        alias_dict = dict()
        with open(
            self.get_remote_filepath(invoice_settings.alias_remote_filepath)
        ) as f:
            for line in f:
                pi_alias_info = line.strip().split(",")
                alias_dict[pi_alias_info[0]] = pi_alias_info[1:]

        return alias_dict

    @functools.lru_cache
    def load_dataframe(self, filepath: str) -> pandas.DataFrame:
        return pandas.read_csv(filepath)

    @functools.lru_cache
    def _load_pi_config(self, filepath: str) -> PIList:
        with open(filepath) as file:
            pi_list = yaml.safe_load(file)

        return PIList.model_validate(pi_list)

    def get_nonbillable_pis(self) -> list[str]:
        pi_list = self._load_pi_config(invoice_settings.nonbillable_pis_filepath)
        return [pi.name for pi in pi_list.root if pi.non_billed_su_types is None]

    def get_pi_non_billed_su_types(self) -> dict[str, list[str]]:
        """PI usernames -> list of SU types that receive credit (zeroed out)."""
        pi_list = self._load_pi_config(invoice_settings.nonbillable_pis_filepath)
        return {
            pi.name: [su.name for su in pi.non_billed_su_types.root]
            for pi in pi_list.root
            if pi.non_billed_su_types is not None
        }

    @functools.lru_cache
    def get_nonbillable_projects(self) -> pandas.DataFrame:
        """
        Returns dataframe of nonbillable projects for current invoice month
        The dataframe has 4 columns: Project Name, Cluster, Is Timed, Is Billable Override
        1. Project Name: Name of the nonbillable project
        2. Cluster: Name of the cluster for which the project is nonbillable, or None meaning all clusters
        3. Is Timed: Boolean indicating if the nonbillable status is time-bound
        4. Is Billable Override: Optional boolean override from projects.yaml
           indicating whether matching projects should be treated as billable
        """

        def _is_in_time_range(start: datetime.date, end: datetime.date) -> bool:
            # Leveraging inherent lexicographical order of YYYY-MM strings
            invoice_date = datetime.datetime.strptime(
                invoice_settings.invoice_month, "%Y-%m"
            ).date()
            return start <= invoice_date <= end

        project_list = []
        with open(invoice_settings.nonbillable_projects_filepath) as file:
            data = yaml.safe_load(file)
        projects = ExcludedProjectList.model_validate(data)
        for project in projects.root:
            project_name = project.name
            cluster_list = project.clusters.root
            is_billable = project.is_billable

            if project.start:
                if not _is_in_time_range(project.start, project.end):
                    continue

                if cluster_list:
                    for cluster in cluster_list:
                        project_list.append(
                            (project_name, cluster.name, True, is_billable)
                        )
                else:
                    project_list.append((project_name, None, True, is_billable))
            elif cluster_list:
                for cluster in cluster_list:
                    if cluster.start:
                        if _is_in_time_range(cluster.start, cluster.end):
                            project_list.append(
                                (project_name, cluster.name, True, is_billable)
                            )
                    elif not cluster.start:
                        project_list.append(
                            (project_name, cluster.name, False, is_billable)
                        )
            else:
                project_list.append((project_name, None, False, is_billable))

        return pandas.DataFrame(
            project_list,
            columns=[
                invoice.NONBILLABLE_PROJECT_NAME,
                invoice.NONBILLABLE_CLUSTER_NAME,
                invoice.NONBILLABLE_IS_TIMED,
                invoice.NONBILLABLE_IS_BILLABLE_OVERRIDE,
            ],
        )

    def get_nonbillable_timed_projects(self) -> list[tuple[str, str]]:
        """Returns list of projects that should be excluded based on dates"""
        nonbilable_projects = self.get_nonbillable_projects()
        return list(
            nonbilable_projects[nonbilable_projects[invoice.NONBILLABLE_IS_TIMED]][
                [invoice.NONBILLABLE_PROJECT_NAME, invoice.NONBILLABLE_CLUSTER_NAME]
            ].itertuples(index=False, name=None)
        )


loader = Loader()
