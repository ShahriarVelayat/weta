from collections import OrderedDict

from AnyQt import QtWidgets, QtGui
from Orange.widgets import gui
from Orange.widgets.settings import Setting

from weta.gui.spark_environment import SparkEnvironment

class Parameter:
    def __init__(self, type, default_value, label, description='', items=None, data_column=False):
        self.type = type
        self.default_value = default_value
        self.label = label
        self.description = description
        self.items = items
        self.data_column = data_column


class SparkBase(SparkEnvironment):
    """
    Base Widget: mainly handle parameter settings
    """

    want_main_area = False
    resizing_enabled = True

    box_text = ''
    doc = ''

    parameters = OrderedDict()

    def __init__(self):
        super(SparkBase, self).__init__()

        # Create parameters Box.
        self.v_main_box = gui.widgetBox(self.controlArea, orientation='horizontal', addSpace=True)

        self.v_setting_box = gui.widgetBox(self.v_main_box, self.box_text, addSpace=True)
        self.v_help_box = gui.widgetBox(self.v_main_box, 'Documentation', addSpace=True)

        self.v_main_box.setMinimumHeight(500)
        self.v_setting_box.setMaximumWidth(250)
        self.v_help_box.setMinimumWidth(400)

        # Create doc info.
        self.v_doc_text = QtWidgets.QTextEdit('<pre>' + self.doc + '</pre>', self.v_help_box)
        self.v_doc_text.setAcceptRichText(True)
        self.v_doc_text.setReadOnly(True)
        self.v_doc_text.autoFormatting()
        self.v_doc_text.setFont(QtGui.QFont('Menlo, Consolas, Courier', 11))

        self.v_help_box.layout().addWidget(self.v_doc_text)

        # info area
        self.v_info_box = gui.widgetBox(self.v_setting_box, 'Info:', addSpace=True)

        # setting area
        self.v_parameters_box = gui.widgetBox(self.v_setting_box, 'Parameters:', addSpace=True)

        for name, parameter in self.parameters.items():
            if parameter.items is not None:
                gui.comboBox(self.v_parameters_box, self, name, label=parameter.label, labelWidth=300,
                             valueType=parameter.type, items=parameter.items)
            elif parameter.type == bool:
                gui.checkBox(self.v_parameters_box, self, name, label=parameter.label, labelWidth=300)
            elif parameter.data_column:
                items = tuple([parameter.default_value])
                gui.comboBox(self.v_parameters_box, self, name, label=parameter.label, labelWidth=300,
                             valueType=parameter.type, items=items, editable=True, sendSelectedValue=True)
            else:
                gui.lineEdit(self.v_parameters_box, self, name, parameter.label, labelWidth=300,
                             valueType=parameter.type)

        self.v_apply_button = gui.button(self.v_setting_box, self, 'Apply', self.apply)
        self.v_apply_button.setEnabled(False)

    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)

        # handle parameter settings
        if getattr(cls, 'parameters') is not None:
            for name, parameter in cls.parameters.items():
                setattr(cls, name, Setting(parameter.default_value, name=name))

    def handleNewSignals(self):
        self.apply()

    def _validate_input(self):
        return True

    def _validate_parameters(self):
        return True

    def apply(self):
        self.clear_messages()
        if not self._validate_input() or not self._validate_parameters():
            return

        params = {name: getattr(self, name) for name, parameter in self.parameters.items()}
        self._apply(params)

        self.hide()

    def _apply(self, params):
        pass
