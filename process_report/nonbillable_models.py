import datetime
import pydantic
from typing import Annotated, TypeVar


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
    pass


NonBilledSUTypeList = UniqueObjectList[NonBilledSUType]


class PIParticipant(pydantic.BaseModel):
    name: str = pydantic.Field(alias="username")
    non_billed_su_types: NonBilledSUTypeList | None = None

    model_config = pydantic.ConfigDict(populate_by_name=True)


PIList = UniqueObjectList[PIParticipant]
