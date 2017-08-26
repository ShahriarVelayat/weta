from collections import OrderedDict

from Orange.widgets import widget
from pyspark.ml import feature
from weta.gui.spark_base import Parameter

from weta.gui.spark_estimator import SparkEstimator


class OWIDF(SparkEstimator, widget.OWWidget):
    priority = 11
    name = "IDF"
    description = "Document IDF transformer"
    icon = "../assets/IDF.svg"

    box_text = 'Inverse Document Frequency'

    learner = feature.IDF
    parameters = OrderedDict({
        'minDocFreq': Parameter(int, 0, 'Minimum document frequency'),
        'inputCol': Parameter(str, 'tf', 'Input column', data_column=True),
        'outputCol': Parameter(str, 'idf', 'Output column'),
    })
    input_dtype = 'vector'

    # def _validate_parameters(self):
    #     if not super(OWIDF, self)._validate_parameters():
    #         return False
    #
    #     df = self.input_data_frame
    #     input_column = self.inputCol
    #     output_column = self.outputCol
    #     types = dict(df.dtypes)
    #     if types[input_column] != 'vector':
    #         self.error('Input column must be vector type')
    #         return False
    #     elif output_column in df.columns:
    #         self.error('Output column must not override an existing one')
    #         return False
    #     else:
    #         return True
