from collections import OrderedDict

from Orange.widgets import widget
from pyspark.ml import feature
from weta.gui.spark_base import Parameter

from weta.gui.spark_estimator import SparkEstimator


class OWStringIndexer(SparkEstimator, widget.OWWidget):
    priority = 12
    name = "StringIndexer"
    description = "StringIndexer"
    icon = "../assets/OneHotEncoder.svg"

    box_text = 'StringIndexer'
    input_dtype = 'string'

    learner = feature.StringIndexer
    parameters = OrderedDict({
        'inputCol': Parameter(str, 'category', 'Input column (%s)' % input_dtype, data_column=True),
        'outputCol': Parameter(str, 'category_index', 'Output column'),
    })