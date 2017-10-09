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

    learner = weta.core.nltk_tokenizer.NLTKTokenizer

    class Parameters:
        inputCol = Parameter(str, 'text', 'Input column', input_column=True, input_dtype=Parameter.STRING)
        outputCol = Parameter(str, 'tokens', 'Output column', output_column=True)
        # 'minTokenLength': Parameter(int, 1, 'Minimum token length'),
        # 'removePunctuation': Parameter(bool, True, 'Remove punctuation?'),
        # 'stem': Parameter(bool, True, 'Stem?'),
        # 'toLowercase': Parameter(bool, True, 'Convert to lower case?')


