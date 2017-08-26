from Orange.widgets import widget
from weta.gui.spark_base import Parameter
from weta.gui.spark_transformer import SparkTransformer
from collections import OrderedDict
from pyspark.ml import Transformer


class SparkGenericTransformation(SparkTransformer, widget.OWWidget):
    name = "Transformation"
    description = "A Generic Transformer of the Spark ml api"
    icon = "../assets/Transformation.svg"

    input_transformer = None

    class Inputs(SparkTransformer.Inputs):
        transformer = widget.Input("Transformer", Transformer)

    parameters = OrderedDict({
        'inputCol': Parameter(str, 'input', 'Input column', data_column=True),
        'outputCol': Parameter(str, 'output', 'Output column'),
    })

    @Inputs.transformer
    def set_input_transformer(self, transformer):
        self.input_transformer = transformer

    def _validate_input(self):
        if not super(SparkGenericTransformation, self)._validate_input():
            return False

        if self.input_transformer is None:
            self.error('Input Transformer does not exist')
            return False

        self.input_dtype = self.input_transformer.input_dtype
        return True

    def _apply(self, params):
        transformer = self.input_transformer
        self.output_data_frame = transformer.transform(self.input_data_frame)
        self.Outputs.data_frame.send(self.output_data_frame)