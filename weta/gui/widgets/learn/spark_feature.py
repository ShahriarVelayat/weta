from Orange.widgets import widget
from pyspark.ml import feature
from collections import OrderedDict
from ..spark_estimator import SparkEstimator
from ..spark_transformer import Parameter, setup_parameter_settings


class OWSparkIDF(SparkEstimator, widget.OWWidget):
    priority = 3
    name = "IDF"
    description = "Document IDF transformer"
    icon = "../assets/IDF.svg"

    box_text = 'Inverse Document Frequency'

    learner_class = feature.IDF
    learner_parameters = OrderedDict({
        'minDocFreq': Parameter(int, 0, 'Minimal document frequency'),
        'inputCol': Parameter(str, 'tf', 'Input column', data_column=True),
        'outputCol': Parameter(str, 'idf', 'Output column'),
    })
    setup_parameter_settings(learner_parameters)
