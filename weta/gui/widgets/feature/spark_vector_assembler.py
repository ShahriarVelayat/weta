from collections import OrderedDict

from pyspark.ml import feature
from gensim.models import doc2vec
from Orange.widgets import widget
from pyspark.ml.linalg import Vectors

from weta.gui.spark_base import Parameter

from weta.gui.spark_estimator import SparkTransformer


class OWVectorAssembler(SparkTransformer, widget.OWWidget):
    priority = 16
    name = "Vector Assembler"
    description = "VectorAssembler"
    icon = "../assets/VectorAssembler.svg"

    box_text = 'VectorAssembler'
    input_dtype = 'vector'

    learner = feature.VectorAssembler
    parameters = OrderedDict({
        'inputCols': Parameter(str, 'vectors', 'Input columns (%s)' % input_dtype, data_column=True),
        'outputCol': Parameter(str, 'assembled_vector', 'Output1 column'),
    })