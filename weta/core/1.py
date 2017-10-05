from weta.core.weta_lib import *


def pyspark_script_console1(inputs, settings):
    in_object = inputs['in_object']
    out_object = None
    return out_object


def pyspark_script_console6(inputs, settings):
    in_object = inputs['in_object']
    out_object = None
    select_cols = ['_id', 'ordinalDate', 'time', 'entryLocation', 'entryPoint', 'dayOfWeek', 'northingEasting', 'messy',
                   'signature', 'propertySecure', 'propertyStolenWordnet', 'vector']

    df = in_object

    out_object = df.select(*select_cols)
    return out_object


def pyspark_script_console10(inputs, settings):
    in_object = inputs['in_object']
    out_object = None
    out_object = in_object.select('_id', 'vector2')
    return out_object


# Data Reader
inputs0 = {}
settings0 = {'file_path': '/Users/Chao/nzpolice/summer/reports.csv', 'format': 'com.databricks.spark.csv'}
outputs0 = dataframe_reader(inputs0, settings0)

# Preprocess
inputs1 = {'in_object': outputs0['DataFrame']}
settings1 = {}
outputs1 = pyspark_script_console1(inputs1, settings1)

# NLTK Tokenizer
inputs4 = {'DataFrame': outputs1['out_object']}
settings4 = {'inputCol': '_id', 'outputCol': 'methodOfEntry_tokens'}
outputs4 = spark_nltk_tokenizer(inputs4, settings4)

# Stopwords Remover
inputs5 = {'DataFrame': outputs4['DataFrame']}
settings5 = {'caseSensitive': False, 'inputCol': 'methodOfEntry_tokens', 'outputCol': 'tokens1'}
outputs5 = spark_stopwords_remover(inputs5, settings5)

# NGram
inputs7 = {'DataFrame': outputs5['DataFrame']}
settings7 = {'inputCol': 'tokens1', 'n': 2, 'outputCol': 'twogram'}
outputs7 = spark_ngram(inputs7, settings7)

# Count Vectorizer (1)
inputs8 = {'DataFrame': outputs7['DataFrame']}
settings8 = {'binary': False, 'inputCol': 'twogram', 'minDF': 1.0, 'minTF': 1.0, 'outputCol': 'vector3',
             'vocabSize': 262144}
outputs8 = spark_count_vectorizer(inputs8, settings8)

# IDF (1)
inputs9 = {'DataFrame': outputs8['DataFrame']}
settings9 = {'inputCol': 'vector2', 'minDocFreq': 0, 'outputCol': 'idf2'}
outputs9 = spark_idf(inputs9, settings9)

# PySpark Script (1)
inputs10 = {'in_object': outputs9['DataFrame']}
settings10 = {}
outputs10 = pyspark_script_console10(inputs10, settings10)

# Count Vectorizer
inputs3 = {'DataFrame': outputs5['DataFrame']}
settings3 = {'binary': False, 'inputCol': 'tokens1', 'minDF': 1.0, 'minTF': 1.0, 'outputCol': 'vector',
             'vocabSize': 262144}
outputs3 = spark_count_vectorizer(inputs3, settings3)

# IDF
inputs2 = {'DataFrame': outputs3['DataFrame']}
settings2 = {'inputCol': 'vector', 'minDocFreq': 0, 'outputCol': 'idf'}
outputs2 = spark_idf(inputs2, settings2)

# PySpark Script
inputs6 = {'in_object': outputs2['DataFrame']}
settings6 = {}
outputs6 = pyspark_script_console6(inputs6, settings6)

# Data Joiner
inputs12 = {'DataFrame1': outputs6['out_object'], 'DataFrame2': outputs10['out_object']}
settings12 = {'id': '_id'}
outputs12 = dataframe_joiner(inputs12, settings12)

# Data Viewer
inputs11 = {'DataFrame': outputs12['DataFrame']}
settings11 = {}
outputs11 = dataframe_viewer(inputs11, settings11)

# IDF
inputs2 = {'DataFrame': outputs3['DataFrame']}
settings2 = {'inputCol': 'vector', 'minDocFreq': 0, 'outputCol': 'idf'}
outputs2 = spark_idf(inputs2, settings2)

# PySpark Script
inputs6 = {'in_object': outputs2['DataFrame']}
settings6 = {}
outputs6 = pyspark_script_console6(inputs6, settings6)

# Data Joiner
inputs12 = {'DataFrame1': outputs6['out_object'], 'DataFrame2': outputs10['out_object']}
settings12 = {'id': '_id'}
outputs12 = dataframe_joiner(inputs12, settings12)

# Data Viewer
inputs11 = {'DataFrame': outputs12['DataFrame']}
settings11 = {}
outputs11 = dataframe_viewer(inputs11, settings11)

# PySpark Script
inputs6 = {'in_object': outputs2['DataFrame']}
settings6 = {}
outputs6 = pyspark_script_console6(inputs6, settings6)

# Data Joiner
inputs12 = {'DataFrame1': outputs6['out_object'], 'DataFrame2': outputs10['out_object']}
settings12 = {'id': '_id'}
outputs12 = dataframe_joiner(inputs12, settings12)

# Data Viewer
inputs11 = {'DataFrame': outputs12['DataFrame']}
settings11 = {}
outputs11 = dataframe_viewer(inputs11, settings11)

# Data Joiner
inputs12 = {'DataFrame1': outputs6['out_object'], 'DataFrame2': outputs10['out_object']}
settings12 = {'id': '_id'}
outputs12 = dataframe_joiner(inputs12, settings12)

# Data Viewer
inputs11 = {'DataFrame': outputs12['DataFrame']}
settings11 = {}
outputs11 = dataframe_viewer(inputs11, settings11)

# Count Vectorizer
inputs3 = {'DataFrame': outputs5['DataFrame']}
settings3 = {'binary': False, 'inputCol': 'tokens1', 'minDF': 1.0, 'minTF': 1.0, 'outputCol': 'vector',
             'vocabSize': 262144}
outputs3 = spark_count_vectorizer(inputs3, settings3)

# IDF
inputs2 = {'DataFrame': outputs3['DataFrame']}
settings2 = {'inputCol': 'vector', 'minDocFreq': 0, 'outputCol': 'idf'}
outputs2 = spark_idf(inputs2, settings2)

# PySpark Script
inputs6 = {'in_object': outputs2['DataFrame']}
settings6 = {}
outputs6 = pyspark_script_console6(inputs6, settings6)

# Data Joiner
inputs12 = {'DataFrame1': outputs6['out_object'], 'DataFrame2': outputs10['out_object']}
settings12 = {'id': '_id'}
outputs12 = dataframe_joiner(inputs12, settings12)

# Data Viewer
inputs11 = {'DataFrame': outputs12['DataFrame']}
settings11 = {}
outputs11 = dataframe_viewer(inputs11, settings11)
