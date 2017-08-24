import pyspark.sql
from collections import OrderedDict
from Orange.widgets import widget, gui, settings

from ..spark_environment import SparkEnvironment

class Parameter:
    def __init__(self, name, default_value='', type='str', widget_type='text_edit', data=None):
        self.name = name
        self.default_value = default_value
        self.type = type
        self.widget_type = widget_type
        self.data = data

class OWDataFrameReader(SparkEnvironment, widget.OWWidget):
    priority = 1

    name = 'Data Frame Reader'
    description = 'Read supported format'
    icon = "../icons/Table.svg"

    class Inputs:
        pass

    class Outputs:
        data_frame = widget.Output('DataFrame', pyspark.sql.DataFrame)

    FORMAT_LIST = tuple([
        ('CSV', 'com.databricks.spark.csv'),
        ('LibSVM', 'libsvm'),
    ])
    OPTIONS_LIST = [
        Parameter('header', 'true', 'Include Header?', 'str')
    ]
    setting_format = settings.Setting('com.databricks.spark.csv')
    setting_file_path = settings.Setting('/Users/Chao/cars.csv')
    setting_parameters = settings.Setting(OrderedDict())

    want_main_area = False

    def __init__(self):
        super().__init__()
        gui.comboBox(self.controlArea, self, 'setting_format', items=OWDataFrameReader.FORMAT_LIST, label='File format')
        gui.lineEdit(self.controlArea, self, 'setting_file_path', label='File path')
        gui.button(self.controlArea, self, 'Apply', callback=self.commit)

    def commit(self):
        df = self.sqlContext.read.format(self.setting_format) \
            .options(header='true', inferschema='true') \
            .load(self.setting_file_path)
        self.Outputs.data_frame.send(df)
        self.hide()
