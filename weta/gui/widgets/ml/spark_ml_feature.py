from Orange.widgets import widget
from pyspark.ml import feature

from weta.gui.base.spark_ml_transformer import OWSparkTransformer


class OWSparkMLFeature(OWSparkTransformer, widget.OWWidget):
    priority = 6
    name = "Feature"
    description = "Features"
    icon = "../icons/FeatureConstructor.svg"

    module = feature
    module_name = 'feature'
    box_text = "Spark Feature Transformers"
