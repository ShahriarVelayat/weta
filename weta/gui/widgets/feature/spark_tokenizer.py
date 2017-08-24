from Orange.widgets import widget
from pyspark.ml import feature
from collections import OrderedDict
from ..spark_estimator import SparkTransformer
from ..spark_transformer import Parameter, setup_parameter_settings


class OWTokenizer(SparkTransformer, widget.OWWidget):
    priority = 1
    name = "Tokenizer"
    description = "Tokenizer"
    icon = "../assets/Tokenizer.svg"

    box_text = 'Tokenizer'

    learner_class = feature.Tokenizer
    learner_parameters = OrderedDict({
        'inputCol': Parameter(str, 'text', 'Input column', data_column=True),
        'outputCol': Parameter(str, 'tokens', 'Output column'),
    })
    setup_parameter_settings(learner_parameters)