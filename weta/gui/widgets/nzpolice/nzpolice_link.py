from Orange.widgets import widget
import pyspark.sql
from weta.gui.spark_transformer import SparkBase


class OWNZPoliceLinkage(SparkBase, widget.OWWidget):
    priority = 2
    name = 'Linkage'
    description = 'link reports within a group and random selected reports in other groups'
    icon = "../assets/Linkage.svg"

    DataFrame = None

    class Inputs:
        DataFrame = widget.Input('DataFrame', pyspark.sql.DataFrame)

    class Outputs:
        DataFrame = widget.Output('DataFrame', pyspark.sql.DataFrame)

    @Inputs.DataFrame
    def set_data_frame(self, df):
        self.DataFrame = df

    def _validate_input(self):
        if self.DataFrame is None:
            self.v_apply_button.setEnabled(False)
            self.error('Input data frame does not exist')
            return False

        return True
    