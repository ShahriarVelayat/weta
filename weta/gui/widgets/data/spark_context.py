from collections import OrderedDict

from Orange.widgets import widget, gui
from Orange.widgets.settings import Setting
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext, HiveContext

from weta.gui.spark_environment import SparkEnvironment
from weta.gui.utils.gui_utils import ParameterWidget


class OWSparkContext(SparkEnvironment, widget.OWWidget):
    priority = 0
    name = "Spark Config"
    description = "Configure the shared contexts: SparkContext (sc), SqlContext (sqlContext) and  HiveContext (hc)"
    icon = "../icons/spark.png"

    want_main_area = False
    resizing_enabled = True
    saved_gui_params = Setting(OrderedDict())

    conf = None

    def __init__(self):
        super().__init__()

        # The main label of the Control's GUI.
        # gui.label(self.controlArea, self, "Spark Context")

        self.conf = SparkConf()
        all_prefedined = dict(self.conf.getAll())
        # Create parameters Box.
        box = gui.widgetBox(self.controlArea, "Spark Application", addSpace = True)

        self.gui_parameters = OrderedDict()

        main_parameters = OrderedDict()
        main_parameters['spark.app.name'] = 'weta_workflow'
        main_parameters['spark.master'] = 'local' # 'yarn'
        main_parameters["spark.executor.instances"] = "8"
        main_parameters["spark.executor.cores"] = "4"
        main_parameters["spark.executor.memory"] = "8g"
        main_parameters["spark.driver.cores"] = "4"
        main_parameters["spark.driver.memory"] = "2g"
        main_parameters["spark.logConf"] = "false"
        main_parameters["spark.app.id"] = "dummy"

        for k, v in self.saved_gui_params.items():
            main_parameters[k] = v

        for k, v in main_parameters.items():
            default_value = all_prefedined.setdefault(k, v)
            self.gui_parameters[k] = ParameterWidget(parent_widget=box, label=k, default_value=v)
            all_prefedined.pop(k)

        for k, v in all_prefedined.items():
            self.gui_parameters[k] = ParameterWidget(parent_widget=box, label=k, default_value=str(v))

        action_box = gui.widgetBox(box)
        # Action Button
        self.create_sc_btn = gui.button(action_box, self, label='Submit', callback=self.create_context)

    def onDeleteWidget(self):
        if self.sc:
            self.sc.stop()

    def create_context(self):
        if self.sc:
            self.sc.stop()

        for key, parameter in self.gui_parameters.items():
            self.conf.set(key, parameter.get_value())
            self.saved_gui_params[key] = parameter.get_value()

        self.sc = SparkContext(conf=self.conf)
        self.sqlContext = SQLContext(self.sc)
        self.hc = HiveContext(self.sc)
        self.hide()
