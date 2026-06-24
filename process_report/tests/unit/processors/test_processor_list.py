from process_report.process_report import PROCESSING_ORDER
from process_report.tests.base import BaseTestCase


class TestProcessorList(BaseTestCase):
    def test_processing_order_column_dependencies(self):
        initialized_columns = set()

        for processor_class in PROCESSING_ORDER:
            operates_on = getattr(processor_class, "operates_on_columns", [])
            initializes = getattr(processor_class, "initializes_columns", [])

            # Check that no columns are initalized more than once
            for column in initializes:
                assert column.name not in initialized_columns, (
                    f"Column '{column.name}' initialized by {processor_class.__name__} but already initialized by a previous processor"
                )

            for column in initializes:
                initialized_columns.add(column.name)

            # Check that all operated on columns have been initialized by a previous processor
            for column in operates_on:
                assert column.name in initialized_columns, (
                    f"Column '{column.name}' operated on by {processor_class.__name__} but not initialized by itself or any previous processor"
                )
