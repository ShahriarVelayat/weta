import pyspark.sql
from Orange.widgets import widget, gui
import pyspark
from Orange.widgets import widget, gui, settings
from AnyQt import QtWidgets

from ..spark_environment import SparkEnvironment


class OWDataFrameViewer(SparkEnvironment, widget.OWWidget):
    # --------------- Widget metadata protocol ---------------
    priority = 2

    name = "Data Frame Viewer"
    description = "View Spark Data frame"
    icon = "../icons/Table.svg"

    # --------------- Input/Output signals ---------------
    class Inputs:
        data_frame = widget.Input("DataFrame", pyspark.sql.DataFrame)

    class Outputs:
        data_frame = widget.Output("DataFrame", pyspark.sql.DataFrame)

    # --------------- UI layout settings ---------------
    want_control_area = False

    # --------------- Settings ---------------

    def __init__(self):
        super().__init__()
        self.ui_view = QtWidgets.QTextEdit('', self.mainArea)
        self.ui_view.setMinimumWidth(300)
        self.ui_view.setMinimumHeight(400)

    @Inputs.data_frame
    def set_input_data_frame(self, df):
        self.Inputs.data_frame.data = df

    # called after received all inputs
    def handleNewSignals(self):
        self.commit()

    # this is the logic: computation, update UI, send outputs. ..
    def commit(self):
        if self.Inputs.data_frame.data is not None:
            df = self.Inputs.data_frame.data
            self.ui_view.setText(df._jdf.showString(20, 20))
            self.Outputs.data_frame.send(df)