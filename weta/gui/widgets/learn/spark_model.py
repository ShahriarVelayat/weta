from Orange.widgets import widget
from pyspark.ml import Model

from weta.gui.base.spark_transformer import SparkTransformer


class SparkModelTransformer(SparkTransformer, widget.OWWidget):
    name = "Model Transformer"
    description = "A Model Transformer of the Spark ml api"
    icon = "../assets/ModelTransformer.svg"

    # ------- extra input --------------
    input_model = None

    class Inputs(SparkTransformer.Inputs):
        model = widget.Input("Model", Model)

    @Inputs.model
    def set_input_model(self, model):
        self.input_model = model

    def _validate_input(self):
        if not super(SparkModelTransformer, self)._validate_input():
            return False

        if self.input_model is None:
            self.error('Input model does not exist')
            return False
        else:
            return True

    def _apply(self, params):
        learner = self.input_model
        self.output_data_frame = learner.transform(self.input_data_frame)
        self.Outputs.data_frame.send(self.output_data_frame)

