from Orange.widgets import widget
from pyspark.ml import Model

from .spark_ml_transformer import OWSparkTransformer
from weta.gui.utils.ml_api_utils import get_estimators


class OWSparkEstimator(OWSparkTransformer):
    name = "Estimator"
    description = "An Estimator of the Spark ml api"
    icon = "icons/spark.png"
    out_model = None
    outputs = [("Model", Model, widget.Dynamic)]

    get_modules = get_estimators

    def go(self, method_instance, paramMap):
        self.out_model = method_instance.fit(self.input_data_frame, params=paramMap)

