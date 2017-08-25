from collections import OrderedDict

from Orange.widgets import widget
from pyspark.ml import feature
from weta.gui.base.spark_base import Parameter

from weta.gui.base.spark_estimator import SparkEstimator


class OWCountVectorizer(SparkEstimator, widget.OWWidget):
    priority = 11
    name = "Count Vectorizer"
    description = "Count Vectorizer"
    icon = "../assets/OneHotEncoder.svg"

    box_text = 'Count Vectorizer'

    learner = feature.CountVectorizer
    parameters = OrderedDict({
        'inputCol': Parameter(str, 'tokens', 'Input column', data_column=True),
        'outputCol': Parameter(str, 'feature', 'Output1 column'),
        'minTF': Parameter(float, 1.0, 'Minimum term frequency'),
        'minDF': Parameter(float, 1.0, 'Minimum document frequency'),
        'vocabSize': Parameter(int, 1 << 18, 'Vocabulary size'),
        'binary': Parameter(bool, False, 'Binary'),
    })

    def _validate_parameters(self):
        if not super(OWCountVectorizer, self)._validate_parameters():
            return False

        df = self.input_data_frame
        input_column = self.inputCol
        output_column = self.outputCol
        types = dict(df.dtypes)
        if types[input_column] != 'array<string>':
            self.error('Input column must be array<string> type')
            return False
        elif output_column in df.columns:
            self.error('Output column must not override an existing one')
            return False
        else:
            return True