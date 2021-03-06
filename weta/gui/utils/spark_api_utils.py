import inspect

from pyspark.sql import DataFrame


def get_dataframe_function_info(func_name):
    sig = inspect.getmembers(DataFrame, inspect.isfunction)
    funcs = dict(sig)
    k = func_name
    v = funcs[k]
    full_description = "<!DOCTYPE html><html><body>"
    full_description += "<h4>" + k + str(inspect.signature(v)) + "</h4>"
    parts = str(inspect.getdoc(v)).split('>>>')[0].strip().split(':param')
    full_description += "<p>" + parts[0] + "<p>"
    full_description += "<h6> Parameters: </h6>"
    full_description += "<ul>"
    for param in parts[1:]:
        full_description += "<li>" + param + "</li>"

    full_description += "</ul>"
    full_description += "</body></html>"

    return full_description
