import pytest
import os
from chispa.column_comparer import assert_column_equality
import pyspark.sql.functions as F
from chispa.dataframe_comparer import *
from lib.utils import *

sparkConf=get_spark_app_config()

spark = SparkSession \
        .builder \
        .config(conf=sparkConf) \
        .getOrCreate()

def test_renameColumns():
    source_data = [
        (1,"1wjtPamAZeGhRnZfhBAHHHjNvnHefd2V2","visa-electron"),
        (2,"1Js9BA1rV31hJFmN25rh8HWfrrYLXAyw9T","jcb")
    ]
    source_df = spark.createDataFrame(source_data, ["id","btc_a","cc_t"])

    colDict = {'id': 'client_identifier',
               'btc_a': 'bitcoin_address',
               'cc_t': 'credit_card_type'}

    actualDf = renameColumns(source_df, colDict)

    expected_data = [
        (1,"1wjtPamAZeGhRnZfhBAHHHjNvnHefd2V2","visa-electron"),
        (2,"1Js9BA1rV31hJFmN25rh8HWfrrYLXAyw9T","jcb")
    ]
    expectedDf = spark.createDataFrame(expected_data, ["client_identifier", "bitcoin_address","credit_card_type"])

    assert_df_equality(actualDf, expectedDf,ignore_row_order=True)


def test_ConfigurationFileExists():
    assert os.path.exists("conf/config.ini") == True,'Configuration File Missing'



def test_filterData():
    source_data = [
        (1, "1wjtPamAZeGhRnZfhBAHHHjNvnHefd2V2", "visa-electron"),
        (2, "1Js9BA1rV31hJFmN25rh8HWfrrYLXAyw9T", "jcb"),
        (5, "1DHTzZ7ypu3EzWtLBFiWoRo9svd1STMyrg","jcb"),
        (7,"1J71SRGqUjhqPuHaZaG8wEtKdNRaKUiuzm","switch"),
        (11, "1HcqQ5Ys77sJm3ZJvxEAPLjCzB8LXVBTHU", "visa")
    ]
    source_df = spark.createDataFrame(source_data, ["client_identifier", "bitcoin_address", "credit_card_type"])


    actualDf = filterData(source_df,"credit_card_type",["switch","visa","visa-electron"])

    expected_data = [
        (1, "1wjtPamAZeGhRnZfhBAHHHjNvnHefd2V2", "visa-electron"),
        (7, "1J71SRGqUjhqPuHaZaG8wEtKdNRaKUiuzm", "switch"),
        (11, "1HcqQ5Ys77sJm3ZJvxEAPLjCzB8LXVBTHU", "visa")
    ]
    expectedDf = spark.createDataFrame(expected_data, ["client_identifier", "bitcoin_address", "credit_card_type"])

    assert_df_equality(actualDf, expectedDf, ignore_row_order=True)
