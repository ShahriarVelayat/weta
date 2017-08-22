from Orange.widgets import widget
from pyspark.ml import recommendation

from weta.gui.base.spark_ml_estimator import OWSparkEstimator


class OWSparkMLRecommendation(OWSparkEstimator, widget.OWWidget):
    priority = 4
    name = "Recommendation"
    description = "recommendation algorithms"
    icon = "../icons/Scattermap.svg"

    module = recommendation
    module_name = 'recommendation'
    box_text = "Spark Recommendation Algorithms"
