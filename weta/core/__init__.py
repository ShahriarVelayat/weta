import importlib

__all__ = ['shared', 'nzpolice']

def find_func(name):
    for module_name in __all__:
        module = importlib.import_module('weta.core.%s' % module_name)
        if hasattr(module, name):
            return getattr(module, name)

    raise "Cannot find function %s in weta.core.*" % name