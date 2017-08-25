import pyspark
from Orange.widgets import widget, gui
from .spark_base import SparkBase


class SparkTransformer(SparkBase):

    # -----------Inputs / Outputs ---------------------
    input_data_frame = None

    class Inputs:
        data_frame = widget.Input("DataFrame", pyspark.sql.DataFrame)

    output_data_frame = None

    class Outputs:
        data_frame = widget.Output("DataFrame", pyspark.sql.DataFrame)

    learner = None  # type: type

    # -------------- Layout config ---------------
    want_main_area = False
    resizing_enabled = True

    def __init__(self):
        super(SparkTransformer, self).__init__()

        self.doc = self.learner.__doc__ if self.learner is not None else ''

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
                    combo.setCurrentIndex(columns.index(saved_value))
                    # setattr(self, name, saved_value)
            return True

    def _apply(self, params):
        learner = self.learner()
        learner.setParams(**params)
        self.output_data_frame = learner.transform(self.input_data_frame)
        self.Outputs.data_frame.send(self.output_data_frame)
