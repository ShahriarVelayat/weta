import concurrent.futures
import logging
from functools import partial

from Orange.widgets import widget, gui
from Orange.widgets.settings import Setting
from Orange.widgets.utils.concurrent import (
    ThreadExecutor, FutureWatcher
)
from PyQt5.QtCore import QThread, pyqtSlot
from weta.core import weta_lib
from weta.gui.spark_environment import SparkEnvironment


class Parameter:

    STRING = 'string'
    ARRAY_STRING = 'array<string>'
    VECTOR = 'vector'

    def __init__(self, type, default_value, label, description='', items=None, input_column=False, output_column=False, input_dtype=None):
        self.type = type
        self.default_value = default_value
        self.label = label
        self.description = description
        self.items = items
        self.input_column = input_column
        self.input_dtype = input_dtype
        self.output_column = output_column


class SparkBase(SparkEnvironment):
    """
    Base Widget: mainly handle parameter settings
    """

    want_main_area = False
    resizing_enabled = True

    box_text = ''

    class Inputs:
        pass

    class Outpus:
        pass

    class Parameters:
        pass

    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)

        # handle parameter settings

        for name, parameter in cls.get_class_variables(cls, 'Parameters', Parameter).items():
            setattr(cls, name, Setting(parameter.default_value, name=name, tag=parameter))

    def __init__(self):
        super(SparkBase, self).__init__()

        # Create parameters Box.
        self.v_main_box = gui.widgetBox(self.controlArea, orientation='horizontal', addSpace=True)
        self.v_setting_box = gui.widgetBox(self.v_main_box, self.box_text if self.box_text != '' else self.name, addSpace=True)

        self.v_main_box.setMinimumHeight(500)
        self.v_setting_box.setMaximumWidth(250)

        # info area
        self.v_info_box = gui.widgetBox(self.v_setting_box, 'Info:', addSpace=True)

        # setting area
        self.v_parameters_box = gui.widgetBox(self.v_setting_box, 'Parameters:', addSpace=True)

        self.initParametersUI()

        self.v_apply_button = gui.button(self.v_setting_box, self, 'Apply', self.apply)
        self.v_apply_button.setEnabled(False)

        #: The current evaluating task (if any)
        self._task = None  # type: Optional[Task]
        #: An executor we use to submit learner evaluations into a thread pool
        self._executor = ThreadExecutor()

    def initParametersUI(self):

        for name, parameter in self.get_class_variables(self, 'Parameters', Parameter).items():
            if parameter.items is not None:
                gui.comboBox(self.v_parameters_box, self, name, label=parameter.label, labelWidth=300,
                             valueType=parameter.type, items=parameter.items)
            elif parameter.type == bool:
                gui.checkBox(self.v_parameters_box, self, name, label=parameter.label, labelWidth=300)
            elif parameter.input_column:
                items = tuple([parameter.default_value])
                gui.comboBox(self.v_parameters_box, self, name, label=parameter.label, labelWidth=300,
                             valueType=parameter.type, items=items, editable=True, sendSelectedValue=True)
            else:
                gui.lineEdit(self.v_parameters_box, self, name, parameter.label, labelWidth=300,
                             valueType=parameter.type)

    def handleNewSignals(self):
        self.apply()

    def _validate_input(self):
        return True

    def _validate_parameters(self):
        return True

    @staticmethod
    def get_class_variables(obj, cls_name, type):
        if not hasattr(obj, cls_name):
            return {}
        else:
            cls = getattr(obj, cls_name)
            return {name: getattr(cls, name) for name in dir(cls) if isinstance(getattr(cls, name), type)}

    def apply(self):
        if self._task is not None:
            # First make sure any pending tasks are cancelled.
            self.cancel()
        assert self._task is None

        self.clear_messages()
        if not self._validate_input() or not self._validate_parameters():
            return

        # hide window first
        self.hide()

        # collect params
        params = {name: getattr(self, name) for name, parameter in self.get_class_variables(self, 'Parameters', Parameter).items()}
        # self._apply(params)

        # collect inputs
        inputs = {}
        for input_name, input_var in self.get_class_variables(self, 'Inputs', widget.Input):
            assert input_name == input_var.name
            input_value = getattr(self, input_name) if hasattr(self, input_name) else None
            inputs[input_name] = input_value

        func = getattr(weta_lib, self.__module__.split('.')[-1])

        _apply_func = partial(func, inputs, params)

        self._task = task = Task()
        # set_progress = methodinvoke(self, "setProgressValue", (float,))

        # def callback(finished):
        #     # check if the task has been cancelled and raise an exception
        #     # from within. This 'strategy' can only be used with code that
        #     # properly cleans up after itself in the case of an exception
        #     # (does not leave any global locks, opened file descriptors, ...)
        #     if task.cancelled:
        #         raise KeyboardInterrupt()
        #     set_progress(finished * 100)

        # _apply_func = partial(_apply_func, callback=callback)

        self.progressBarInit()
        # Submit the evaluation function to the executor and fill in the
        # task with the resultant Future.
        task.future = self._executor.submit(_apply_func)
        # Setup the FutureWatcher to notify us of completion
        task.watcher = FutureWatcher(task.future)
        # by using FutureWatcher we ensure `_task_finished` slot will be
        # called from the main GUI thread by the Qt's event loop
        task.watcher.done.connect(self._task_finished)


    @pyqtSlot(concurrent.futures.Future)
    def _task_finished(self, f):
        """
        Parameters
        ----------
        f : Future
            The future instance holding the result of learner evaluation.
        """
        assert self.thread() is QThread.currentThread()
        assert self._task is not None
        assert self._task.future is f
        assert f.done()

        self._task = None
        self.progressBarFinished()

        try:
            results = f.result()  # type: dict
            assert isinstance(results, dict)
            # collect outputs
            outputs = {}
            for output_name, output_var in self.get_class_variables(self, 'Outputs', widget.Output).items():
                assert output_name == output_var.name
                outputs[output_name] = output_var
            for output_name, output_value in results.items():
                outputs[output_name].send(output_value)

        except Exception as ex:
            # Log the exception with a traceback
            log = logging.getLogger()
            log.exception(__name__, exc_info=True)
            self.error("Exception occurred during evaluation: {!r}"
                       .format(ex))

    def cancel(self):
        """
        Cancel the current task (if any).
        """
        if self._task is not None:
            self._task.cancel()
            assert self._task.future.done()
            # disconnect the `_task_finished` slot
            self._task.watcher.done.disconnect(self._task_finished)
            self._task = None

    def onDeleteWidget(self):
        self.cancel()
        super().onDeleteWidget()


class Task:
    """
    A class that will hold the state for an learner evaluation.
    """
    #: A concurrent.futures.Future with our (eventual) results.
    #: The OWLearningCurveC class must fill this field
    future = ...  # type: concurrent.futures.Future

    #: FutureWatcher. Likewise this will be filled by OWLearningCurveC
    watcher = ...  # type: FutureWatcher

    #: True if this evaluation has been cancelled. The OWLearningCurveC
    #: will setup the task execution environment in such a way that this
    #: field will be checked periodically in the worker thread and cancel
    #: the computation if so required. In a sense this is the only
    #: communication channel in the direction from the OWLearningCurve to the
    #: worker thread
    cancelled = False  # type: bool

    def cancel(self):
        """
        Cancel the task.

        Set the `cancelled` field to True and block until the future is done.
        """
        # set cancelled state
        self.cancelled = True
        # cancel the future. Note this succeeds only if the execution has
        # not yet started (see `concurrent.futures.Future.cancel`) ..
        self.future.cancel()
        # ... and wait until computation finishes
        concurrent.futures.wait([self.future])

