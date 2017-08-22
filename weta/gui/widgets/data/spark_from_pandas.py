import pandas
import pyspark
from Orange.widgets import widget, gui, settings
from AnyQt.QtWidgets import QSizePolicy

from weta.gui.base.spark_environment import SparkEnvironment


class OWSparkToPandas(SparkEnvironment, widget.OWWidget):
    priority = 8
    name = "from Pandas"
    description = "Convert Pandas dataframe to Spark DataFrame."
    icon = "../icons/spark.png"

    inputs = [("DataFrame", pandas.DataFrame, "get_input", widget.Default)]
    outputs = [("DataFrame", pyspark.sql.DataFrame, widget.Dynamic)]
    settingsHandler = settings.DomainContextHandler()

    def __init__(self):
        super().__init__()
        gui.label(self.controlArea, self, "Pandas->Spark:")
        self.setSizePolicy(QSizePolicy.Maximum, QSizePolicy.Maximum)

    def get_input(self, obj):
        self.send("DataFrame", self.hc.createDataFrame(obj))
