from Orange.widgets import widget
from pyspark.ml import classification

from weta.gui.base.spark_ml_estimator import OWSparkEstimator


class OWSparkMLClassification(OWSparkEstimator, widget.OWWidget):
    priority = 1
    name = "Classification"
    description = "Classification algorithms"
    icon = "../icons/Category-Classify.svg"

    module = classification
    module_name = 'classification'
    box_text = "Spark Classification Algorithms"
