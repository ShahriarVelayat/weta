from Orange.widgets import widget
from pyspark.ml import regression

from weta.gui.widgets.spark_ml_estimator import OWSparkEstimator
from ..spark_ml_transformer import Algorithm, Parameter


class OWSparkMLRegression(OWSparkEstimator, widget.OWWidget):
    priority = 3
    name = "Regression"
    description = "regression algorithms"
    icon = "../icons/Regression.svg"

    # module = regression
    # module_name = 'regression'
    box_text = "Spark Regression Algorithms"

    algorithms = [
        Algorithm('LinearRegression', regression.LinearRegression, init_parameters=[
            Parameter('featureCol', str, 'feature', 'Feature column'),
            Parameter('labelCol', str, 'label', 'Label column'),
            Parameter('maxIter', int, 100, 'Maximal iteration'),
            Parameter('regParam', float, 0.0, 'Regression Parameter'),
            Parameter('elasticNetParam', float, 0.0, 'Elastic Net Parameter'),
            Parameter('tol', float, 1e-6, 'tol'),
            Parameter('fitIntercept', bool, True, 'Fit intercept'),
            Parameter('standardization', bool, True, 'Standardization'),
            Parameter('solver', str, 'auto', 'Solver'),
            Parameter('weightCol', str, None, 'Weight Column'),
            Parameter('aggregationDepth', int, 2, 'Aggregation depth')
        ]),
        Algorithm('IsotonicRegression', regression.IsotonicRegression, init_parameters=[
            Parameter('featureCol', str, 'feature', 'Feature column')
        ]),
    ]

    def foo(self):
        self.error()
