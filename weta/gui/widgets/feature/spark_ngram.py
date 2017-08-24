from Orange.widgets import widget
from pyspark.ml import feature
from collections import OrderedDict
from weta.gui.widgets.spark_estimator import SparkTransformer
from weta.gui.widgets.spark_transformer import Parameter, setup_parameter_settings


class OWNGram(SparkTransformer, widget.OWWidget):
    priority = 2
    name = "NGram"
    description = "NGram"
    icon = "../assets/NGram.svg"

    box_text = 'NGram'

    learner_class = feature.NGram
    learner_parameters = OrderedDict({
        'n': Parameter(int, 2, 'N'),
        'inputCol': Parameter(str, 'text', 'Input column', data_column=True),
        'outputCol': Parameter(str, 'tokens', 'Output column'),
    })
    setup_parameter_settings(learner_parameters)