from collections import OrderedDict

import pyspark.sql
from PyQt5 import QtWidgets, QtCore
from Orange.widgets import widget, gui, settings

from weta.gui.spark_environment import SparkEnvironment


class Parameter:
    def __init__(self, name, default_value='', type='str', widget_type='text_edit', data=None):
        self.name = name
        self.default_value = default_value
        self.type = type
        self.widget_type = widget_type
        self.data = data


class OWDataFrameReader(SparkEnvironment, widget.OWWidget):
    priority = 1

    name = 'Data Reader'
    description = 'Read supported file format to a DataFrame'
    icon = "../assets/FileReader.svg"

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
    setting_file_path = settings.Setting('')
    setting_parameters = settings.Setting(OrderedDict())

    want_main_area = False

    def __init__(self):
        super().__init__()
        self.controlArea.setMinimumWidth(400)
        gui.comboBox(self.controlArea, self, 'setting_format', items=OWDataFrameReader.FORMAT_LIST, label='File format')
        file_browser_box = gui.hBox(self.controlArea, 'File path')
        gui.lineEdit(file_browser_box, self, 'setting_file_path', orientation=QtCore.Qt.Horizontal)
        gui.toolButton(file_browser_box, self, 'Browse...', callback=self.browse_file)
        gui.button(self.controlArea, self, 'Apply', callback=self.apply)

    def browse_file(self):
        file = QtWidgets.QFileDialog.getOpenFileName()[0]
        if file:
            self.controls.setting_file_path.setText(file)

    def apply(self):
        df = self.sqlContext.read.format(self.setting_format) \
            .options(header='true', inferschema='true') \
            .load(self.setting_file_path)
        self.Outputs.data_frame.send(df)
        self.hide()
