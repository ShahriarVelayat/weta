from Orange.widgets import widget
from pyspark.ml import Model

from .spark_transformer import SparkTransformer


class SparkEstimator(SparkTransformer):
    name = "Estimator"
    description = "An Estimator of the Spark ml api"
    icon = "icons/spark.png"
    output_model = None

    class Outputs:
        model = widget.Output("Model", Model)

    def _apply(self, learner, params):
        self.out_model = learner.fit(self.input_data_frame, params=params)
        self.Outputs.model.send(self.output_model)

