from Orange.widgets import widget
from pyspark.ml import regression
from collections import OrderedDict
from ..spark_estimator import SparkEstimator
from ..spark_transformer import Parameter, setup_parameter_settings


class OWLinearRegression(SparkEstimator, widget.OWWidget):
    priority = 101
    name = "Linear Regression"
    description = "Linear Regression Algorithm"
    icon = "../assets/LinearRegression.svg"

    box_text = "Linear Regression"

    learner_class = regression.LinearRegression
    learner_parameters = OrderedDict({
        'featureCol': Parameter(str, 'feature', 'Feature column', data_column=True),
        'labelCol': Parameter(str, 'label', 'Label column', data_column=True),
        'maxIter': Parameter(int, 100, 'Maximal iteration'),
        'regParam': Parameter(float, 0.0, 'Regression Parameter'),
        'elasticNetParam': Parameter(float, 0.0, 'Elastic Net Parameter'),
        'tol': Parameter(float, 1e-6, 'tol'),
        'fitIntercept': Parameter(bool, True, 'Fit intercept'),
        'standardization': Parameter(bool, False, 'Standardization'),
        'solver': Parameter(str, 'auto', 'Solver'),
        'weightCol': Parameter(str, None, 'Weight Column'),
        'aggregationDepth': Parameter(int, 2, 'Aggregation depth')
    })
    setup_parameter_settings(learner_parameters)
