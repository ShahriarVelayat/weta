from collections import OrderedDict

from Orange.widgets import widget
from pyspark.ml import feature
from weta.gui.spark_base import Parameter

from weta.gui.spark_estimator import SparkTransformer


class OWStopWordsRemover(SparkTransformer, widget.OWWidget):
    priority = 4
    name = "Stopwords Remover"
    description = "StopWords Remover"
    icon = "../assets/StopWordsRemover.svg"

    box_text = 'StopWords Remover'
    input_dtype = 'array<string>'

    learner = feature.StopWordsRemover
    parameters = OrderedDict({
        'inputCol': Parameter(str, 'text', 'Input column (%s)' % input_dtype, data_column=True),
        'outputCol': Parameter(str, 'tokens', 'Output column'),
        # 'stopWords': Parameter(list, None, 'Stopwords list'),
        'caseSensitive': Parameter(bool, False, 'Case sensitive'),
    })

    def _apply(self, params):
        feature.StopWordsRemover.loadDefaultStopWords('english')  # TODO: default load english stop words
        super(OWStopWordsRemover, self)._apply(params)