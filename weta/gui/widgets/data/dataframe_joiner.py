from Orange.widgets import widget
from pyspark.sql import DataFrame
from weta.gui.spark_base import SparkBase, Parameter
from collections import OrderedDict


class OWDataFrameJoiner(SparkBase, widget.OWWidget):

    priority = 10

    name = 'Data Joiner'
    description = 'Join two DataFrame into one in x axis'
    icon = '../assets/DataFrameJoiner.svg'

    input_data_frame1 = None  # type: DataFrame
    input_data_frame2 = None  # type: DataFrame
    class Inputs:
        data_frame1 = widget.Input('DataFrame1', DataFrame, id='df1')
        data_frame2 = widget.Input('DataFrame2', DataFrame, id='df2')

    output_data_frame = None
    class Outputs:
        data_frame = widget.Output('DataFrame', DataFrame)

    parameters = OrderedDict({
        'id': Parameter(str, '_id', 'ID column to join on', data_column=True),
        # 'test_weight': Parameter(float, 0.1, 'Test weight of split ratio'),
    })

    @Inputs.data_frame1
    def set_input_data_frame1(self, data_frame):
        self.input_data_frame1 = data_frame

    @Inputs.data_frame2
    def set_input_data_frame2(self, data_frame):
        self.input_data_frame2 = data_frame

    def _validate_input(self):
        if not super(OWDataFrameJoiner, self)._validate_input():
            return False

        if self.input_data_frame1 is None or self.input_data_frame2 is None:
            self.output_data_frame = None
            self.v_apply_button.setEnabled(False)
            self.error('Input data frame does not exist')
            return False
        else:
            self.v_apply_button.setEnabled(True)
            return True

    def _validate_parameters(self):
        if not super(OWDataFrameJoiner, self)._validate_parameters():
            return False

        if getattr(self, 'id') is not None:
            id_column = self.id
            if id_column not in self.input_data_frame1.columns or id_column not in self.input_data_frame2.columns:
                self.error('id column is not of input data frame')
                return False

        return True

    def _apply(self, params):
        self.output_data_frame = self.input_data_frame1.join(self.input_data_frame2, [params['id']])
        self.Outputs.data_frame.send(self.output_data_frame)
