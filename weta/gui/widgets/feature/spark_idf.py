from collections import OrderedDict

from Orange.widgets import widget
from pyspark.ml import feature
from weta.gui.base.spark_base import Parameter

from weta.gui.base.spark_estimator import SparkEstimator


class OWIDF(SparkEstimator, widget.OWWidget):
    priority = 21
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


