from collections import OrderedDict

from Orange.widgets import widget
from pyspark.ml import feature
from weta.gui.spark_base import Parameter

from weta.gui.spark_estimator import SparkTransformer


class OWHashingTF(SparkTransformer, widget.OWWidget):
    priority = 1
    name = "Hashing TF"
    description = "Hashing TF"
    icon = "../assets/CountVectorizer.svg"

    box_text = 'Hashing TF'
    input_dtype = 'array<string>'

    learner = feature.HashingTF
    parameters = OrderedDict({
        'inputCol': Parameter(str, 'tokens', 'Input column (%s)' % input_dtype, data_column=True),
        'outputCol': Parameter(str, 'vector', 'Output column'),
        'numFeatures': Parameter(int, 1 << 18, 'Number of features'),
        'binary': Parameter(bool, False, 'Binary'),
    })