from Orange.widgets import widget
from pyspark.ml import Model
from collections import OrderedDict

from weta.gui.spark_base import Parameter
from weta.gui.spark_transformer import SparkTransformer


class OWModelTransformation(SparkTransformer, widget.OWWidget):
    name = "Model Transformation"
    description = "A Model Transformer of the Spark ml api"
    icon = "../assets/ModelTransformation.svg"

    input_transformer = None

    class Inputs(SparkTransformer.Inputs):
        transformer = widget.Input("Model", Model)

    class Outputs(SparkTransformer.Outputs):
        transformer = widget.Output("Model", Model)

    parameters = OrderedDict({
        'inputCol': Parameter(str, 'input', 'Input column', data_column=True),
        'outputCol': Parameter(str, 'output', 'Output column'),
    })

    @Inputs.transformer
    def set_input_transformer(self, transformer):
        self.input_transformer = transformer

    def _validate_input(self):
        if not super(OWModelTransformation, self)._validate_input():
            return False

        if self.input_transformer is None:
            self.error('Input Model does not exist')
            return False

        # if self.inputCol not in self.input_data_frame.columns:
        #     self.inputCol = self.input_transformer.inputCol
        self.input_dtype = self.input_transformer.input_dtype
        return True

    def _apply(self, params):
        transformer = self.input_transformer
        self.output_data_frame = transformer.transform(self.input_data_frame)
        self.Outputs.data_frame.send(self.output_data_frame)
