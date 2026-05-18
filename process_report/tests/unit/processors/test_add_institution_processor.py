from unittest import TestCase, mock
import pandas

from process_report.tests import util as test_utils
from process_report.institute_list_models import InstituteList


class TestAddInstitutionProcessor(TestCase):
    def _get_test_data(self, pi_names, projects=None):
        if projects is None:
            projects = [f"Project{i}" for i in range(len(pi_names))]
        return pandas.DataFrame(
            {
                "Manager (PI)": pi_names,
                "Project - Allocation": projects,
                "Institution": ["" for _ in pi_names],
            }
        )

    @mock.patch("process_report.util.load_institute_list")
    def test_add_institution(self, mock_load_institute_list):
        """Institution column is populated from the PI's email domain."""

        mock_load_institute_list.return_value = InstituteList.model_validate(
            [
                {"display_name": "Boston University", "domains": ["bu.edu"]},
                {"display_name": "MIT", "domains": ["mit.edu"]},
            ]
        )

        test_data = self._get_test_data(
            pi_names=["pi1@bu.edu", "pi2@mit.edu"],
            projects=["ProjectA", "ProjectB"],
        )
        processor = test_utils.new_add_institution_processor(data=test_data)
        processor.process()
        output = processor.data
        assert output.loc[0, "Institution"] == "Boston University"
        assert output.loc[1, "Institution"] == "MIT"

    @mock.patch("process_report.util.load_institute_list")
    def test_add_institution_missing_pi(self, mock_load_institute_list):
        """Rows with no PI are skipped without raising an error."""
        mock_load_institute_list.return_value = InstituteList.model_validate(
            [
                {"display_name": "Boston University", "domains": ["bu.edu"]},
            ]
        )

        test_data = self._get_test_data(
            pi_names=[None, "pi@bu.edu"],
            projects=["ProjectA", "ProjectB"],
        )

        processor = test_utils.new_add_institution_processor(data=test_data)
        processor.process()  # should not raise
        output = processor.data

        assert pandas.isna(output.loc[0, "Manager (PI)"])
        assert output.loc[1, "Institution"] == "Boston University"
