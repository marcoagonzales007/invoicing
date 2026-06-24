from dataclasses import dataclass, field

from process_report.loader import loader
from process_report.invoices import invoice
from process_report.processors import processor


@dataclass
class LenovoProcessor(processor.Processor):
    su_charge_info: dict = field(default_factory=loader.get_lenovo_su_charge_info)

    initializes_columns = (invoice.SU_CHARGE_COLUMN, invoice.LENOVO_CHARGE_COLUMN)
    operates_on_columns = (
        *initializes_columns,
        invoice.SU_TYPE_COLUMN,
        invoice.SU_HOURS_COLUMN,
    )

    def _apply_su_charge(self, data):
        for su_name, su_charge in self.su_charge_info.items():
            if su_name in data:
                return su_charge
        return 0

    def _process(self):
        self.data[invoice.SU_CHARGE_FIELD] = self.data[invoice.SU_TYPE_FIELD].apply(
            self._apply_su_charge
        )
        self.data[invoice.LENOVO_CHARGE_FIELD] = (
            self.data[invoice.SU_HOURS_FIELD] * self.data[invoice.SU_CHARGE_FIELD]
        )
