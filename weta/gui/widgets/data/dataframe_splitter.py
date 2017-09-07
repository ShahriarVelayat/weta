from Orange.widgets import widget
from pyspark.sql import DataFrame
from weta.gui.spark_base import SparkBase, Parameter
from collections import OrderedDict


class OWDataFrameSplitter(SparkBase, widget.OWWidget):

    priority = 6

    name = 'Data Splitter'
    description = 'Split a DataFrame into train and test datasets with a fixed ratio'

    input_data_frame = None
    class Inputs:
        data_frame = widget.Input('DataFrame', DataFrame)

    class Outputs:
        train_data_frame = widget.Output('DataFrame1', DataFrame, id='train')
        test_data_frame = widget.Output('DataFrame2', DataFrame, id='test')

    parameters = OrderedDict({
        'train_weight': Parameter(float, 0.9, 'Train weight of split ratio'),
        'test_weight': Parameter(float, 0.1, 'Test weight of split ratio'),
    })

    @Inputs.data_frame
    def set_input_data_frame(self, data_frame):
        self.input_data_frame = data_frame

    def _validate_input(self):
        if not super(OWDataFrameSplitter, self)._validate_input():
            return False

        if self.input_data_frame is None:
            self.output_data_frame = None
            self.v_apply_button.setEnabled(False)
            self.error('Input data frame does not exist')
        else:
            self.v_apply_button.setEnabled(True)
            return True

    def _apply(self, params):
        train, test = self.input_data_frame.randomSplit([self.train_weight, self.test_weight], seed=12345)
        self.Outputs.train_data_frame.send(train)
        self.Outputs.test_data_frame.send(test)
