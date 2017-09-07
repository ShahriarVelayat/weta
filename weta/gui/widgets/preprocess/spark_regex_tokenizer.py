from collections import OrderedDict

from Orange.widgets import widget
from pyspark.ml import feature
from weta.gui.spark_base import Parameter

from weta.gui.spark_estimator import SparkTransformer


class OWRegexTokenizer(SparkTransformer, widget.OWWidget):
    priority = 1
    name = "Regex Tokenizer"
    description = "Regex Tokenizer"
    icon = "../assets/RegexTokenizer.svg"

    box_text = 'Regex Tokenizer'
    input_dtype = 'string'

    learner = feature.RegexTokenizer
    parameters = OrderedDict({
        'inputCol': Parameter(str, 'text', 'Input column (%s)' % input_dtype, data_column=True),
        'outputCol': Parameter(str, 'tokens', 'Output column'),
        'minTokenLength': Parameter(int, 1, 'Minimum token length'),
        'gaps': Parameter(bool, True, 'Gaps?'),
        'pattern': Parameter(str, '\\s+', 'Pattern'),
        'toLowercase': Parameter(bool, True, 'Convert to lower case?')
    })
