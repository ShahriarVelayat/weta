from Orange.widgets import widget
from pyspark.ml import Model

from .spark_transformer import SparkTransformer


class SparkEstimator(SparkTransformer):

    output_model = None

    class Outputs(SparkTransformer.Outputs):
        model = widget.Output('Model', Model)

    def _apply(self, params):
        learner = self.learner()
        learner.setParams(**params)
        self.output_model = learner.fit(self.input_data_frame)
        self.Outputs.data_frame.send(self.input_data_frame)
        self.Outputs.model.send(self.output_model)

