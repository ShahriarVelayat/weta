from Orange.widgets import widget
from pyspark.ml import feature
from collections import OrderedDict
from weta.gui.widgets.spark_estimator import SparkTransformer
from weta.gui.widgets.spark_transformer import Parameter, setup_parameter_settings


class OWOneHotEncoder(SparkTransformer, widget.OWWidget):
    priority = 11
    name = "One-Hot Encoder"
    description = "One-Hot Encoder"
    icon = "../assets/OneHotEncoder.svg"

    box_text = 'One Hot Encoder'

    learner_class = feature.OneHotEncoder
    learner_parameters = OrderedDict({
        'dropLast': Parameter(bool, True, 'Minimal document frequency'),
        'inputCol': Parameter(str, 'indexed', 'Input column', data_column=True),
        'outputCol': Parameter(str, 'features', 'Output column'),
    })
    setup_parameter_settings(learner_parameters)