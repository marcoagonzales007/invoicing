from unittest import TestCase, mock
import pandas

from process_report.tests import util as test_utils
from process_report.institute_list_models import InstituteList


class TestAddInstitutionProcessor(TestCase):
    def _get_test_data(self, pi_names, projects, institutions=None):
        if institutions is None:
            institutions = ["" for i in pi_names]
        return pandas.DataFrame(
            {
                "Manager (PI)": pi_names,
                "Project - Allocation": projects,
                "Institution": institutions,
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
        answer_data = self._get_test_data(
            pi_names=["pi1@bu.edu", "pi2@mit.edu"],
            projects=["ProjectA", "ProjectB"],
            institutions=["Boston University", "MIT"],
        )
        assert output.equals(answer_data)

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
        answer_data = self._get_test_data(
            pi_names=[None, "pi@bu.edu"],
            projects=["ProjectA", "ProjectB"],
            institutions=["", "Boston University"],
        )
        assert output.equals(answer_data)
