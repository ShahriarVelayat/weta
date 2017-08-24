import pyspark
from AnyQt import QtCore, QtWidgets, QtGui
from Orange.widgets import widget, gui
from Orange.widgets.settings import Setting
from collections import OrderedDict

from weta.gui.widgets.spark_environment import SparkEnvironment


class Parameter:
    def __init__(self, type, default_value, label, description='', items=None, data_column=False):
        self.type = type
        self.default_value = default_value
        self.label = label
        self.description = description
        self.items = items
        self.data_column = data_column


def setup_parameter_settings(learner_parameters):
    for name, parameter in learner_parameters.items():
        setattr(SparkTransformer, name, Setting(parameter.default_value))


class SparkTransformer(SparkEnvironment):#, widget.OWWidget):
    # widget_id = None

    name = "Transformer"
    description = "A Transformer of the Spark ml api"
    icon = "icons/spark.png"

    # -----------Inputs / Outputs ---------------------
    input_data_frame = None

    class Inputs:
        data_frame = widget.Input("DataFrame", pyspark.sql.DataFrame)

    output_data_frame = None

    class Outputs:
        data_frame = widget.Output("DataFrame", pyspark.sql.DataFrame)

    learner_class = None  # type: type
    learner_parameters = OrderedDict()  # type: OrderedDict[str, Parameter]
    # -------------- Settings --------------------

    # -------------- Layout config ---------------
    want_main_area = False
    resizing_enabled = True

    box_text = ''

    def __init__(self):
        # Create parameters Box.
        self.v_main_box = gui.widgetBox(self.controlArea, orientation='horizontal', addSpace=True)
        self.v_setting_box = gui.widgetBox(self.v_main_box, self.box_text, addSpace=True)
        self.v_help_box = gui.widgetBox(self.v_main_box, 'Documentation', addSpace=True)
        self.v_main_box.setMinimumHeight(500)
        self.v_setting_box.setMaximumWidth(300)
        self.v_help_box.setMinimumWidth(600)

        # Create learner doc.
        self.v_method_info_text = QtWidgets.QTextEdit('<pre>'+self.learner_class.__doc__+'</pre>', self.v_help_box)
        self.v_method_info_text.setAcceptRichText(True)
        self.v_method_info_text.setReadOnly(True)
        self.v_method_info_text.autoFormatting()
        font = QtGui.QFont('Menlo, Consolas, Courier', 11)
        self.v_method_info_text.setFont(font)

        self.v_help_box.layout().addWidget(self.v_method_info_text)

        # info area
        self.v_info_box = gui.widgetBox(self.v_setting_box, 'Info:', addSpace=True)

        # setting area
        self.v_parameters_box = gui.widgetBox(self.v_setting_box, 'Parameters:', addSpace=True)

        for name, parameter in self.learner_parameters.items():
            if parameter.items is not None:
                gui.comboBox(self.v_parameters_box, self, name, label=parameter.label, labelWidth=300,
                             valueType=parameter.type, items=parameter.items)
            elif parameter.type == bool:
                gui.checkBox(self.v_parameters_box, self, name, label=parameter.label, labelWidth=300)
            elif parameter.data_column:
                items = tuple([parameter.default_value])
                gui.comboBox(self.v_parameters_box, self, name, label=parameter.label, labelWidth=300,
                             valueType=parameter.type, items=items, sendSelectedValue=True, editable=True)

            else:
                gui.lineEdit(self.v_parameters_box, self, name, parameter.label, labelWidth=300,
                             valueType=parameter.type)

        self.v_apply_button = gui.button(self.v_setting_box, self, 'Apply', self.apply)
        self.v_apply_button.setEnabled(False)

    @Inputs.data_frame
    def set_input_data_frame(self, data_frame):
        self.input_data_frame = data_frame

    def handleNewSignals(self):
        self.apply()

    def _check_input(self):
        if self.input_data_frame is None:
            self.output_data_frame = None
            self.v_apply_button.setEnabled(False)
            self.error('Input data frame does not exist')
            for name, parameter in self.learner_parameters.items():
                if parameter.data_column:
                    combo = getattr(self.controls, name)
                    combo.setEditable = True
            return False
        else:
            self.v_apply_button.setEnabled(True)
            # update data column combobox
            items = tuple(self.input_data_frame.columns)
            for name, parameter in self.learner_parameters.items():
                if parameter.data_column:
                    saved_value = getattr(self, name)
                    saved_value = saved_value if saved_value in items else items[0]
                    combo = getattr(self.controls, name)
                    combo.setEditable = False
                    combo.clear()
                    combo.addItems(items)
                    # combo.setCurrentIndex(items.index(saved_value))
                    setattr(self, name, saved_value)
            return True

    def validate(self):
        return True

    def apply(self):
        self.clear_messages()
        if not self._check_input() or not self.validate():
            return

        learner = self.learner_class()
        params = {name: getattr(self, name) for name, parameter in self.learner_parameters.items()}
        self._apply(learner, params)
        self.hide()

    def _apply(self, learner, params):
        learner.setParams(**params)
        self.output_data_frame = learner.transform(self.input_data_frame)
        self.Outputs.data_frame.send(self.output_data_frame)
