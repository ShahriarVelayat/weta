import pyspark
from AnyQt import QtGui, QtWidgets, QtCore
from Orange.widgets import widget, gui
from Orange.widgets.settings import Setting
from collections import OrderedDict

from weta.gui.utils.gui_utils import ParameterWidget
from weta.gui.utils.ml_api_utils import get_transformers, get_object_info
from weta.gui.widgets.spark_environment import SparkEnvironment

class Algorithm:
    def __init__(self, name, algorithm_class, init_parameters=[], description='', doc=''):
        self.name = name
        self.algorithm_class = algorithm_class
        self.init_parameters = init_parameters
        self.description = description
        self.doc = doc

class Parameter:
    def __init__(self, name, type, default_value, label, description='', items=None):
        self.name = name
        self.type = type
        self.default_value = default_value
        self.label = label
        self.description = description
        self.items = items

class OWSparkTransformer(SparkEnvironment):
    # widget_id = None

    name = "Transformer"
    description = "A Transformer of the Spark ml api"
    icon = "icons/spark.png"

    class Inputs:
        data_frame = widget.Input("DataFrame", pyspark.sql.DataFrame)

    class Outputs:
        data_frame = widget.Output("DataFrame", pyspark.sql.DataFrame)

    want_main_area = False
    resizing_enabled = True

    conf = None
    input_data_frame = None
    output_data_frame = None
    obj_type = None

    box_text = "Spark Application"

    algorithms = []
    algorithm = None
    algorithm_parameters = []

    get_modules = get_transformers

    setting_parameters = Setting(OrderedDict())
    setting_var_cache = Setting(False)

    def __init__(self):
        super().__init__()
        # gui.label(self.controlArea, self, "pyspark.ml")

        # Create parameters Box.
        self.ui_main_box = gui.widgetBox(self.controlArea, orientation='horizontal', addSpace=True)
        self.ui_setting_box = gui.widgetBox(self.ui_main_box, self.box_text, addSpace=True)
        self.ui_help_box = gui.widgetBox(self.ui_main_box, 'Documentation', addSpace=True)



        self.ui_parameters = OrderedDict()

        # Create place for selecting the method

        algorithm_names = (a.name for a in self.algorithms)
        default_value = self.setting_parameters.get('algorithm', None)
        self.ui_parameters['algorithm'] = ParameterWidget(parent_widget=self.ui_setting_box,
                                                          list_values=algorithm_names,
                                                          default_value=default_value,
                                                          callback_func=self.change_algorithm)

        # Create method label doc.
        self.ui_method_info_text = QtWidgets.QTextEdit('', self.ui_help_box)
        self.ui_method_info_text.setAcceptRichText(True)
        self.ui_method_info_text.setReadOnly(True)
        self.ui_method_info_text.autoFormatting()
        self.ui_help_box.layout().addWidget(self.ui_method_info_text)

        # Create place to show/set parameters of method
        self.ui_parameters_box = gui.widgetBox(self.ui_setting_box, 'Parameters:', addSpace=True)

        self.ui_action_box = gui.widgetBox(self.ui_setting_box)
        self.ui_cache_check = gui.checkBox(self.ui_action_box, self, value='setting_var_cache', label ='cache output DataFrame?')
        # Action Button
        self.ui_create_sc_btn = gui.button(self.ui_action_box, self, label='Apply', callback=self.apply)

        # recover settings
        self.change_algorithm(self.ui_parameters['algorithm'].get_value())

    def change_algorithm(self, text):
        """
        change transformers / estimators
        :param text:
        :return:
        """

        for a in self.algorithms:
            if a.name == text:
                self.algorithm = a
        obj_name = self.algorithm.name
        obj_doc = self.algorithm.doc
        full_description = self.algorithm.description
        self.algorithm_parameters = self.algorithm.init_parameters
        self.ui_method_info_text.setText(full_description)

        # clear a layout and delete all widgets
        # aLayout is some QLayout for instance
        layout = self.ui_parameters_box.layout()
        while layout.count():
            item = layout.takeAt(0)
            item.widget().deleteLater()

        for i, p in enumerate(self.algorithm_parameters):
            default_value = p.default_value
            parameter_doc = p.description
            list_values = None
            name = p.name
            if name.endswith('Col') and self.input_data_frame:
                list_values = list(self.input_data_frame.columns)

            default_value = self.setting_parameters.get(name, default_value)

            self.ui_parameters[name] = ParameterWidget(parent_widget=self.ui_parameters_box, list_values=list_values,
                                                       label=p.label, default_value=str(default_value),
                                                       place_holder_text=parameter_doc,
                                                       doc_text=parameter_doc)
            if list_values and str(default_value) in list_values:
                index = self.ui_parameters[name].widget.findText(str(default_value), QtCore.Qt.MatchFixedString)
                if index >= 0:
                    self.ui_parameters[name].widget.setCurrentIndex(index)

    @Inputs.data_frame
    def set_input_data_frame(self, obj):
        self.input_data_frame = obj
        self.change_algorithm(self.ui_parameters['algorithm'].get_value())

    def build_param_map(self, method_instance):
        paramMap = dict()
        for p in self.algorithm_parameters:
            value = self.ui_parameters[p.name].get_usable_value()
            # name = self.gui_parameters[k].get_param_name(self.method.__name__, k)
            paramMap[pyspark.ml.param.Param(method_instance, p.name, '')] = value
        return paramMap

    def sync_parameters_setting(self):
        for k in self.ui_parameters:
            self.setting_parameters[k] = self.ui_parameters[k].get_value()

    def apply(self):
        self.sync_parameters_setting()
        if self.input_data_frame is None:
            self.error('Input Data Frame not exist')
            return

        algorithm_instance = self.algorithm.algorithm_class()
        paramMap = self.build_param_map(algorithm_instance)
        self.go(algorithm_instance, paramMap)
        if self.setting_var_cache:
            self.output_data_frame = self.output_data_frame.cache()

        self.Outputs.data_frame.send(self.output_data_frame)
        self.hide()

    def go(self, algorithm_instance, paramMap):
        self.output_data_frame = algorithm_instance.transform(self.input_data_frame, params=paramMap)