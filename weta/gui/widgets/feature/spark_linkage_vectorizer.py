from collections import OrderedDict

from Orange.widgets import widget
from pyspark.ml import feature
from weta.gui.spark_base import Parameter

from weta.gui.spark_estimator import SparkEstimator


class OWLinkageVectorizer(SparkEstimator, widget.OWWidget):
    priority = 2
    name = "Linkage Vectorizer"
    description = "Linkage Vectorizer"
    icon = "../assets/LinkageVectorizer.svg"

    box_text = 'Linkage Vectorizer'
    input_dtype = 'array<string>'

    learner = feature.CountVectorizer
    parameters = OrderedDict({
        # 'inputCol': Parameter(str, 'tokens', 'Input column (%s)' % input_dtype, data_column=True),
        # 'outputCol': Parameter(str, 'vector', 'Output1 column'),
        # 'minTF': Parameter(float, 1.0, 'Minimum term frequency'),
        # 'minDF': Parameter(float, 1.0, 'Minimum document frequency'),
        # 'vocabSize': Parameter(int, 1 << 18, 'Vocabulary size'),
        # 'binary': Parameter(bool, False, 'Binary'),
    })