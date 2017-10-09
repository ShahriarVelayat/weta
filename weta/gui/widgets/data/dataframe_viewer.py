import pyspark
import pyspark.sql
from PyQt5 import QtCore
from Orange.widgets import widget, gui
from pyspark.ml.linalg import Vector

from weta.gui.spark_environment import SparkEnvironment


class OWDataFrameViewer(SparkEnvironment, widget.OWWidget):
    # --------------- Widget metadata protocol ---------------
    priority = 2

    name = "Data Viewer"
    description = "View Spark Data frame"
    icon = "../assets/DataFrameViewer.svg"

    # --------------- Input/Output signals ---------------
    input_data_frame: pyspark.sql.DataFrame = None
    class Inputs:
        data_frame = widget.Input("DataFrame", pyspark.sql.DataFrame)

    class Outputs:
        data_frame = widget.Output("DataFrame", pyspark.sql.DataFrame)

    # --------------- UI layout settings ---------------
    want_control_area = True

    # --------------- Settings ---------------

    def __init__(self):
        super().__init__()
        self.controlArea.setMinimumWidth(250)
        self.v_info_box = gui.vBox(self.controlArea, 'Info')
        self.v_info = gui.label(self.v_info_box, self, '')
        self.v_info.setAlignment(QtCore.Qt.AlignTop)

        self.mainArea.setMinimumWidth(600)
        self.mainArea.setMinimumHeight(600)

        self.v_table = gui.table(self.mainArea, 0, 0)

    @Inputs.data_frame
    def set_input_data_frame(self, df):
        self.input_data_frame = df

    # called after received all inputs
    def handleNewSignals(self):
        self.apply()

    def _check_input(self):
        if self.input_data_frame is None:
            self.warning('Input data does not exist')
            return False
        else:
            return True

    # this is the logic: computation, update UI, send outputs. ..
    def apply(self):
        if not self._check_input():
            return

        self.clear_messages()
        df = self.input_data_frame  # type: pyspark.sql.DataFrame

        # show data
        columns = df.columns

        self.v_info.setText(self.get_info())

        self.v_table.setRowCount(0)
        self.v_table.setColumnCount(len(columns))
        self.v_table.setHorizontalHeaderLabels(columns)
        for i, row in enumerate(df.head(n=100)):  # show top 100 rows
            self.v_table.insertRow(i)
            for j, column in enumerate(df.columns):
                value = row[column]
                if isinstance(value, Vector):
                    value = str(list(value.toArray()[:10])) + '...' # to dense array
                else:
                    value = str(value)
                gui.tableItem(self.v_table, i, j, value)

        self.Outputs.data_frame.send(df)

    def get_info(self):
        df = self.input_data_frame
        columns = df.columns
        return '''
Rows: %d 
Columns: %d
Types: 
    %s
       ''' % (df.count(), len(columns), '\n    '.join([t[0] + ' - ' + t[1] for t in df.dtypes]))