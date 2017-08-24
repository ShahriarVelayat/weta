from Orange.widgets import widget
from pyspark.ml import clustering

from weta.gui.widgets.spark_ml_estimator import OWSparkEstimator


class OWSparkMLClustering(OWSparkEstimator, widget.OWWidget):
    priority = 2
    name = "Clustering"
    description = "Clustering algorithms"
    icon = "../icons/KMeans.svg"
    module = clustering
    module_name = 'clustering'
    box_text = "Spark Clustering Algorithms"
