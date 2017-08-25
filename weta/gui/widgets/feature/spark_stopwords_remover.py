from collections import OrderedDict

from Orange.widgets import widget
from pyspark.ml import feature
from weta.gui.base.spark_base import Parameter

from weta.gui.base.spark_estimator import SparkTransformer


class OWStopWordsRemover(SparkTransformer, widget.OWWidget):
    priority = 3
    name = "Stopwords Remover"
    description = "StopWords Remover"
    icon = "../assets/StopWordsRemover.svg"

    box_text = 'StopWords Remover'

    learner = feature.StopWordsRemover
    parameters = OrderedDict({
        'inputCol': Parameter(str, 'text', 'Input column', data_column=True),
        'outputCol': Parameter(str, 'tokens', 'Output column'),
        'stopWords': Parameter(list, None, 'Stopwords list'),
        'caseSensitive': Parameter(bool, False, 'Case sensitive'),
    })

    def _validate_parameters(self):
        if not super(OWStopWordsRemover, self)._validate_parameters():
            return False

        df = self.input_data_frame
        input_column = self.inputCol
        output_column = self.outputCol
        types = dict(df.dtypes)
        if types[input_column] != 'array<string>':
            self.error('Input column must be array<string> type')
            return False
        elif output_column in df.columns:
            self.error('Output column must not override an existing one')
            return False
        else:
            return True
