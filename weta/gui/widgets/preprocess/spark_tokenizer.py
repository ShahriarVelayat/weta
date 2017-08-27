from collections import OrderedDict

from Orange.widgets import widget
from pyspark.ml import feature
from weta.gui.spark_base import Parameter

from weta.gui.spark_estimator import SparkTransformer


class OWTokenizer(SparkTransformer, widget.OWWidget):
    priority = 1
    name = "Tokenizer"
    description = "Tokenizer"
    icon = "../assets/Tokenizer.svg"

    box_text = 'Tokenizer'
    input_dtype = 'string'

    learner = feature.Tokenizer
    parameters = OrderedDict({
        'inputCol': Parameter(str, 'text', 'Input column (%s)' % input_dtype, data_column=True),
        'outputCol': Parameter(str, 'tokens', 'Output column'),
    })


    # def _validate_parameters(self):
    #     if not super(OWTokenizer, self)._validate_parameters():
    #         return False
    #
    #     df = self.input_data_frame
    #     input_column = self.inputCol
    #     output_column = self.outputCol
    #     types = dict(df.dtypes)
    #     if types[input_column] != 'string':
    #         self.error('Input column must be string type')
    #         return False
    #     elif output_column in df.columns:
    #         self.error('Output column must not override an existing one')
    #         return False
    #     else:
    #         return True
