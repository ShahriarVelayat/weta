from collections import OrderedDict

from pyspark.ml import feature
from gensim.models import doc2vec
from Orange.widgets import widget
from pyspark.ml.linalg import Vectors

from weta.gui.spark_base import Parameter

from weta.gui.spark_estimator import SparkEstimator


class OWDoc2Vec(SparkEstimator, widget.OWWidget):
    priority = 13
    name = "Doc2Vec"
    description = "Doc2Vec"
    icon = "../assets/Doc2Vec.svg"

    box_text = 'Doc2Vec'
    input_dtype = 'array<string>'

    learner = None
    parameters = OrderedDict({
        'inputCol': Parameter(str, 'tokens', 'Input column (%s)' % input_dtype, data_column=True),
        'outputCol': Parameter(str, 'vector', 'Output1 column'),
        'vectorSize': Parameter(int, 100, 'Vector size'),
        'minCount': Parameter(int, 5, 'Minimum count'),
        # 'numPartitions': Parameter(int, 1, 'Number of partitions '),
        # 'stepSize': Parameter(float, 0.025, 'Step size'),
        # 'maxIter': Parameter(int, 1, 'Maximum Iteration'),
        # 'seed': Parameter(int, None, 'Seed'),
        'windowSize': Parameter(int, 5, 'Window size'),
        # 'maxSentenceLength': Parameter(int, 1000, 'Maximum sentence length'),
        'workers': Parameter(int, 4, 'Workers')
    })

    def _apply(self, params):
        input_column = self.inputCol
        X = self.input_data_frame.toPandas()[input_column]

        documents = [doc2vec.TaggedDocument(tuples, range(len(X))) for tuples in X]
        model = doc2vec.Doc2Vec(documents, size=self.size, window=window, min_count=min_count, workers=workers)

        vecs = [Vectors.dense(vec) for vec in list(model.docvecs)]

        # self.input_data_frame.with_column(self.outputCol, )