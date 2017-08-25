from collections import OrderedDict

from Orange.widgets import widget
from pyspark.ml import feature
from weta.gui.base.spark_base import Parameter

from weta.gui.base.spark_estimator import SparkTransformer


class OWNGram(SparkTransformer, widget.OWWidget):
    priority = 2
    name = "NGram"
    description = "NGram"
    icon = "../assets/NGram.svg"

    box_text = 'NGram'

    learner = feature.NGram
    parameters = OrderedDict({
        'n': Parameter(int, 2, 'N'),
        'inputCol': Parameter(str, 'text', 'Input column', data_column=True),
        'outputCol': Parameter(str, 'tokens', 'Output column'),
    })