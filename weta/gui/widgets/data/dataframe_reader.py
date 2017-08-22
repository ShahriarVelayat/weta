import pyspark.sql
from Orange.widgets import widget, gui, settings

from weta.gui.widgets.spark_environment import SparkEnvironment


class OWDataFrameReader(SparkEnvironment, widget.OWWidget):
    priority = 1

    name = 'DataFrame Reader'
    description = 'Read supported format'
    icon = "../icons/Table.svg"

    inputs = []

    outputs = [("DataFrame", pyspark.sql.DataFrame, widget.Dynamic)]

    # setting_options = settings.Setting()

    def __init__(self):
        super().__init__()
        gui.label(self.controlArea, self, "File path")

        df = self.sqlContext.read.format('com.databricks.spark.csv') \
            .options(header='true', inferschema='true') \
            .load('/Users/Chao/cars.csv')
        self.send("DataFrame", df)
