import pytest
import pydantic

from process_report.models import nonbillable_models


@pytest.mark.parametrize(
    "excluded_projects",
    [
        [
            {"name": "proj-a"},
            {"name": "proj-b"},
            {"name": "proj-a"},
        ],
    ],
)
def test_duplicate_project_names(excluded_projects):
    with pytest.raises(pydantic.ValidationError, match="found duplicate name"):
        nonbillable_models.ExcludedProjectList.model_validate(excluded_projects)


@pytest.mark.parametrize(
    "excluded_projects",
    [
        [
            {
                "name": "proj-b",
                "clusters": [
                    {"name": "stack"},
                    {"name": "stack"},
                ],
            }
        ]
    ],
)
def test_duplicate_cluster_names(excluded_projects):
    with pytest.raises(pydantic.ValidationError, match="found duplicate name"):
        nonbillable_models.ExcludedProjectList(root=excluded_projects)


@pytest.mark.parametrize(
    "excluded_projects",
    [
        [
            {
                "name": "proj-c",
                "start": "2025-06",  # End date before start date
                "end": "2025-01",
            }
        ]
    ],
)
def test_invalid_project_date_range(excluded_projects):
    with pytest.raises(
        pydantic.ValidationError, match="End date must be after start date"
    ):
        nonbillable_models.ExcludedProjectList(root=excluded_projects)


@pytest.mark.parametrize(
    "excluded_projects",
    [
        [
            {
                "name": "proj-d",
                "start": "2025-06",
            },  # Only start or end date provided
        ],
        [
            {"name": "proj-d", "end": "2025-06"},
        ],
    ],
)
def test_partial_date_range(excluded_projects):
    with pytest.raises(
        pydantic.ValidationError, match="must be provided together or not at all"
    ):
        nonbillable_models.ExcludedProjectList(root=excluded_projects)


@pytest.mark.parametrize(
    "excluded_projects",
    [
        [
            {
                "name": "proj-e",
                "clusters": [{"name": "invalid-cluster"}],
            }
        ]
    ],
)
def test_invalid_cluster_enum(excluded_projects):
    with pytest.raises(pydantic.ValidationError, match="is not a valid cluster name"):
        nonbillable_models.ExcludedProjectList(root=excluded_projects)


@pytest.mark.parametrize(
    "pi_list",
    [
        [
            {
                "username": "user@example.com",
                "non_billed_su_types": [
                    {"name": "OpenStack Storage"},
                    {"name": "OpenStack Storage"},
                ],
            }
        ]
    ],
)
def test_duplicate_non_billed_su_types(pi_list):
    with pytest.raises(pydantic.ValidationError, match="found duplicate name"):
        nonbillable_models.PIList.model_validate(pi_list)


@pytest.mark.parametrize(
    "pi_list",
    [
        [
            {
                "username": "user@example.com",
                "non_billed_su_types": [{"name": "invalid-su-type"}],
            }
        ]
    ],
)
def test_invalid_non_billed_su_type(pi_list):
    with pytest.raises(pydantic.ValidationError, match="is not a valid SU type"):
        nonbillable_models.PIList.model_validate(pi_list)


@pytest.mark.parametrize(
    "pi_list",
    [
        [
            {"username": "user@example.com"},
            {"username": "user@example.com"},
        ],
    ],
)
def test_duplicate_pi_names(pi_list):
    with pytest.raises(pydantic.ValidationError, match="found duplicate name"):
        nonbillable_models.PIList.model_validate(pi_list)
