from collections import OrderedDict

from Orange.widgets import widget
from pyspark.ml import feature
from weta.gui.spark_base import Parameter

from weta.gui.spark_estimator import SparkTransformer


class OWOneHotEncoder(SparkTransformer, widget.OWWidget):
    priority = 2
    name = "One-Hot Encoder"
    description = "One-Hot Encoder"
    icon = "../assets/OneHotEncoder.svg"

    box_text = 'One Hot Encoder'

    learner = feature.OneHotEncoder
    parameters = OrderedDict({
        'dropLast': Parameter(bool, True, 'Drop the last category'),
        'inputCol': Parameter(str, 'tokens', 'Input column', data_column=True),
        'outputCol': Parameter(str, 'features', 'Output column'),
    })

    input_dtype = 'array<string>'

    # def _validate_parameters(self):
    #     if not super(OWOneHotEncoder, self)._validate_parameters():
    #         return False
    #
    #     df = self.input_data_frame
    #     input_column = self.inputCol
    #     output_column = self.outputCol
    #     types = dict(df.dtypes)
    #     if types[input_column] != 'array<string>':
    #         self.error('Input column must be array<string> type')
    #         return False
    #     elif output_column in df.columns:
    #         self.error('Output column must not override an existing one')
    #         return False
    #     else:
    #         return True