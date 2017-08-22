import pyspark.sql
from Orange.widgets import widget, gui
import pyspark
from Orange.widgets import widget, gui, settings
from AnyQt import QtWidgets

from weta.gui.widgets.spark_environment import SparkEnvironment


class OWDataFrameViewer(SparkEnvironment, widget.OWWidget):
    priority = 2

    name = "DataFrame Viewer"
    description = "View Spark Data frame"
    icon = "../icons/Table.svg"

    inputs = [("DataFrame", pyspark.sql.DataFrame, "get_input", widget.Default)]
    outputs = [("DataFrame", pyspark.sql.DataFrame, widget.Dynamic)]

    def __init__(self):
        super().__init__()
        self.ui_view = QtWidgets.QTextEdit('', self.mainArea)
        self.ui_view.setMinimumWidth(300)
        self.ui_view.setMinimumHeight(400)

    def get_input(self, df):
        self.ui_view.setText(df._jdf.showString(20, 20))
        self.send("DataFrame", df)