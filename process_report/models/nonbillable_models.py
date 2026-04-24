import datetime
import pydantic
import pandas
from typing import Annotated, TypeVar
from functools import lru_cache
from pathlib import Path

_MODELS_DIR = Path(__file__).parent


@lru_cache
def get_allowed_clusters() -> set[str]:
    with open(_MODELS_DIR / "cluster_names.txt") as f:
        return set(f.read().strip().split("\n"))


@lru_cache
def get_allowed_su_types() -> set[str]:
    with open(_MODELS_DIR / "su_types.txt") as f:
        return set(f.read().strip().split("\n"))


def validate_date(v: str) -> datetime.date:
    return datetime.datetime.strptime(v, "%Y-%m").date()


DateField = Annotated[datetime.date, pydantic.BeforeValidator(validate_date)]


class NamedObject(pydantic.BaseModel):
    name: str


T = TypeVar("T", bound=NamedObject)


class UniqueObjectList(pydantic.RootModel[list[T]]):
    root: list[T]

    @pydantic.model_validator(mode="after")
    def validate_unique_names(self):
        seen: set[str] = set()
        for item in self.root:
            if item.name in seen:
                raise ValueError(f"{item.name}: found duplicate name")
            seen.add(item.name)

        return self


class ExcludedCluster(NamedObject):
    start: DateField | None = None
    end: DateField | None = None
    reason: str | None = None

    @pydantic.field_validator("name")
    def only_allowed_cluster_names(cls, v):
        allowed = get_allowed_clusters()
        if v not in allowed:
            raise ValueError(f"'{v}' is not a valid cluster name")
        return v


ExcludedClusterList = UniqueObjectList[ExcludedCluster]


class ExcludedProject(NamedObject):
    clusters: ExcludedClusterList = ExcludedClusterList([])
    start: DateField | None = None
    end: DateField | None = None
    reason: str | None = None
    is_billable: bool = False

    @pydantic.model_validator(mode="after")
    def validate_time_periods(self):
        def is_date_range_valid(
            start: datetime.date | None, end: datetime.date | None
        ) -> bool:
            if start and end:
                if end < start:
                    raise ValueError(
                        f"{self.name}: End date must be after start date for project"
                    )
            elif start or end:
                raise ValueError(
                    f"{self.name}: Start and end dates must be provided together or not at all"
                )
            return True

        is_date_range_valid(self.start, self.end)
        if self.clusters:
            for excluded_cluster in self.clusters.root:
                is_date_range_valid(excluded_cluster.start, excluded_cluster.end)

        return self


ExcludedProjectList = UniqueObjectList[ExcludedProject]


class NonBilledSUType(NamedObject):
    @pydantic.field_validator("name")
    def only_allowed_su_types(cls, v):
        allowed = get_allowed_su_types()
        if v not in allowed:
            raise ValueError(f"'{v}' is not a valid SU type")
        return v


NonBilledSUTypeList = UniqueObjectList[NonBilledSUType]


class PIParticipant(pydantic.BaseModel):
    name: str = pydantic.Field(alias="username")
    non_billed_su_types: NonBilledSUTypeList | None = None

    model_config = pydantic.ConfigDict(populate_by_name=True)


PIList = UniqueObjectList[PIParticipant]


def get_nonbillable_pis(pi_list: PIList) -> list[str]:
    return [pi.name for pi in pi_list.root if pi.non_billed_su_types is None]


def get_pi_non_billed_su_types(pi_list: PIList) -> dict[str, list[str]]:
    """PI usernames -> list of SU types that receive credit (zeroed out)."""
    return {
        pi.name: [su.name for su in pi.non_billed_su_types.root]
        for pi in pi_list.root
        if pi.non_billed_su_types is not None
    }


def get_nonbillable_projects(
    excluded_projects: ExcludedProjectList, invoice_month: str
) -> pandas.DataFrame:
    """
    Returns dataframe of nonbillable projects for current invoice month
    The dataframe has 4 columns: Project Name, Cluster, Is Timed, Is Billable Override
    1. Project Name: Name of the nonbillable project
    2. Cluster: Name of the cluster for which the project is nonbillable, or None meaning all clusters
    3. Is Timed: Boolean indicating if the nonbillable status is time-bound
    4. Is Billable Override: Optional boolean override from projects.yaml
       indicating whether matching projects should be treated as billable
    """
    invoice_date = datetime.datetime.strptime(invoice_month, "%Y-%m").date()

    def _is_in_time_range(start: datetime.date, end: datetime.date) -> bool:
        # Leveraging inherent lexicographical order of YYYY-MM strings

        return start <= invoice_date <= end

    project_list = []

    for project in excluded_projects.root:
        project_name = project.name
        cluster_list = project.clusters.root
        is_billable = project.is_billable

        if project.start:
            if not _is_in_time_range(project.start, project.end):
                continue

            if cluster_list:
                for cluster in cluster_list:
                    project_list.append((project_name, cluster.name, True, is_billable))
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

    return project_list
