import pyspark.sql
import pyspark.ml
from Orange.widgets import widget, gui
from .spark_base import SparkBase


class SparkTransformer(SparkBase):

    # -----------Inputs / Outputs ---------------------
    input_data_frame = None

    class Inputs:
        data_frame = widget.Input("DataFrame", pyspark.sql.DataFrame)

    output_data_frame = None
    output_transformer = None

    class Outputs:
        data_frame = widget.Output("DataFrame", pyspark.sql.DataFrame)
        transformer = widget.Output("Transformer", pyspark.ml.Transformer)

    learner = None  # type: type
    input_dtype = None

    # -------------- Layout config ---------------
    want_main_area = False
    resizing_enabled = True

    def __init__(self):
        self.doc = self.learner.__doc__ if self.learner is not None else ''
        super(SparkTransformer, self).__init__()

    @Inputs.data_frame
    def set_input_data_frame(self, data_frame):
        self.input_data_frame = data_frame

    def _validate_input(self):
        if not super(SparkTransformer, self)._validate_input():
            return False

        if self.input_data_frame is None:
            self.output_data_frame = None
            self.v_apply_button.setEnabled(False)
            self.error('Input data frame does not exist')
            for name, parameter in self.parameters.items():
                if parameter.data_column:
                    combo = getattr(self.controls, name)
                    combo.setEditable = True
            return False
        else:
            self.v_apply_button.setEnabled(True)
            # update data column combobox
            # types = dict(self.input_data_frame.dtypes)
            columns = self.input_data_frame.columns
            for name, parameter in self.parameters.items():
                if parameter.data_column:
                    saved_value = getattr(self, name)
                    saved_value = saved_value if saved_value in columns else columns[0]
                    combo = getattr(self.controls, name)
                    combo.setEditable = False
                    combo.clear()
                    combo.addItems(columns)
                    # combo.setCurrentIndex(columns.index(saved_value))
                    setattr(self, name, saved_value)
            return True

    def _validate_parameters(self):
        if not super(SparkTransformer, self)._validate_parameters():
            return False

        df = self.input_data_frame
        types = dict(df.dtypes)
        if getattr(self, 'inputCol') is not None:
            input_column = self.inputCol
            if types[input_column] != self.input_dtype:
                self.error('Input column must be %s type' % self.input_dtype)
                return False
        if getattr(self, 'outputCol') is not None:
            output_column = self.outputCol
            if output_column in df.columns:
                self.error('Output column must not override an existing one')
                return False

        return True

    def _apply(self, params):
        transformer = self.learner()
        transformer.setParams(**params)

        self.output_transformer = transformer
        self.output_transformer.input_dtype = self.input_dtype  # attach a required input dtype
        self.output_data_frame = transformer.transform(self.input_data_frame)

        self.Outputs.data_frame.send(self.output_data_frame)
        self.Outputs.transformer.send(self.output_transformer)
