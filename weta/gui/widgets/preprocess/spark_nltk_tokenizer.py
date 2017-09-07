from collections import OrderedDict
import nltk
import string as string_module

from Orange.widgets import widget
import pyspark.ml
from pyspark.sql.functions import udf
from pyspark.sql.types import IntegerType, StringType, ArrayType
from weta.gui.spark_base import Parameter
from pyspark.ml.param.shared import *
import weta.core.nltk_tokenizer
from weta.gui.spark_estimator import SparkTransformer


class OWNLTKTokenizer(SparkTransformer, widget.OWWidget):
    priority = 1
    name = "NLTK Tokenizer"
    description = "NLTK Tokenizer"
    icon = "../assets/NLTKTokenizer.svg"

    box_text = 'NLTK Tokenizer'
    input_dtype = 'string'

    learner = weta.core.nltk_tokenizer.NLTKTokenizer
    parameters = OrderedDict({
        'inputCol': Parameter(str, 'text', 'Input column (%s)' % input_dtype, data_column=True),
        'outputCol': Parameter(str, 'tokens', 'Output column'),
        # 'minTokenLength': Parameter(int, 1, 'Minimum token length'),
        # 'removePunctuation': Parameter(bool, True, 'Remove punctuation?'),
        # 'stem': Parameter(bool, True, 'Stem?'),
        # 'toLowercase': Parameter(bool, True, 'Convert to lower case?')
    })


