from Orange.widgets import widget
from pyspark.ml import Model

from .spark_transformer import SparkTransformer
import copy


class SparkEstimator(SparkTransformer):

    class Outputs(SparkTransformer.Outputs):
        transformer = widget.Output('Model', Model)

    def _apply(self, params):
        estimator = self.learner()  # estimator
        estimator.setParams(**params)
        model = estimator.fit(self.input_data_frame)  # model

        self.output_transformer = model
        self.output_transformer.input_dtype = self.input_dtype  # attach a required input dtype
        self.output_data_frame = model.transform(self.input_data_frame)

        self.Outputs.data_frame.send(self.output_data_frame)
        self.Outputs.transformer.send(self.output_transformer)

