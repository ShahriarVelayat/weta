from pyspark.ml import feature
from pyspark.ml import classification
from pyspark.ml import regression
import ast
import base64
import pickle

import weta.core.nltk_tokenizer

global sqlContext


def spark_transformer(transformer_cls, inputs, settings):
    input_data_frame = inputs['DataFrame']
    params = settings
    transformer = transformer_cls()
    transformer.setParams(**params)
    output_data_frame = transformer.transform(input_data_frame)

    return {
        'DataFrame': output_data_frame,
        'Transformer': transformer
    }


def spark_estimator(estimator_cls, inputs, settings):
    input_data_frame = inputs['DataFrame']
    params = settings
    estimator = estimator_cls()
    estimator.setParams(**params)
    model = estimator.fit(input_data_frame)  # model
    output_dataframe = model.transform(input_data_frame)

    return {
        'DataFrame': output_dataframe,
        'Model': model
    }

#----------------------- preprocess -----------------------

def spark_ngram(inputs, settings):
    return spark_transformer(feature.NGram, inputs, settings)


def spark_nltk_tokenizer(inputs, settings):
    return spark_transformer(weta.core.nltk_tokenizer.NLTKTokenizer, inputs, settings)


def spark_regex_tokenizer(inputs, settings):
    return spark_transformer(feature.RegexTokenizer, inputs, settings)


def spark_stopwords_remover(inputs, settings):
    return spark_transformer(feature.StopWordsRemover, inputs, settings)


def spark_tokenizer(inputs, settings):
    return spark_transformer(feature.Tokenizer, inputs, settings)


#----------------------- learn -----------------------

def spark_decision_tree_classifier(inputs, settings):
    return spark_estimator(classification.DecisionTreeClassifier, inputs, settings)


def spark_linear_regression(inputs, settings):
    return spark_estimator(regression.LinearRegression, inputs, settings)


def spark_logistic_regression(inputs, settings):
    return spark_estimator(classification.LogisticRegression, inputs, settings)


def spark_naive_bayes(inputs, settings):
    return spark_estimator(classification.NaiveBayes, inputs, settings)

#----------------------- data -----------------------

def dataframe_reader(inputs, settings):
    df = sqlContext.read.format(settings['setting_format']) \
        .options(header='true', inferschema='true') \
        .load(settings['setting_file_path'])

    return {'DataFrame': df}

def dataframe_viewer(inputs, settings):
    df = inputs['DataFrame']
    return {'DataFrame': df}