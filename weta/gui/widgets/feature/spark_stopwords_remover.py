from Orange.widgets import widget
from pyspark.ml import feature
from collections import OrderedDict
from weta.gui.widgets.spark_estimator import SparkTransformer
from weta.gui.widgets.spark_transformer import Parameter, setup_parameter_settings


class OWStopWordsRemover(SparkTransformer, widget.OWWidget):
    priority = 3
    name = "Stopwords Remover"
    description = "StopWords Remover"
    icon = "../assets/StopWordsRemover.svg"

    box_text = 'StopWords Remover'

    learner_class = feature.StopWordsRemover
    learner_parameters = OrderedDict({
        'inputCol': Parameter(str, 'text', 'Input column', data_column=True),
        'outputCol': Parameter(str, 'tokens', 'Output column'),
        'stopWords': Parameter(list, None, 'Stopwords list'),
        'caseSensitive': Parameter(bool, False, 'Case sensitive'),
    })
    setup_parameter_settings(learner_parameters)
