import argparse
from pyspark.sql import SparkSession,DataFrame
from pyspark.sql.functions import expr
from pyspark.sql.types import StructType, StructField, DateType, StringType, IntegerType
from lib.utils import *
from lib.logger import get_logger,logLevelDict

Logger = get_logger('Logging')

if __name__ == "__main__":
    #Parse Command Arguments
    parser = argparse.ArgumentParser()

    parser.add_argument('--dataset1', help="Customer Dataset Path",required=True)
    parser.add_argument('--dataset2', help="Financial Dataset Path", required=True)
    parser.add_argument('--countrylist', help="Countries list for filter", required=True)

    args = parser.parse_args()

    sparkConf=get_spark_app_config()

    spark = SparkSession \
        .builder \
        .config(conf=sparkConf) \
        .getOrCreate()

    Logger.info(args)

    dataset1DDL = flightSchemaStruct = StructType([
        StructField("id", IntegerType(),False),
        StructField("first_name", StringType(),False),
        StructField("last_name", StringType(),False),
        StructField("email", StringType(),False),
        StructField("country", StringType(),False)
       ])

    customerDF = spark.read \
    .format("csv") \
    .option("header", "true") \
    .schema(dataset1DDL) \
    .option("mode", "FAILFAST") \
    .load(args.dataset1) \
    .drop('first_name') \
    .drop('last_name')

    Logger.info(customerDF.schema)

    FilteredCustomerDF=filterData(customerDF,'country',args.countrylist.split(','))

    Logger.info(FilteredCustomerDF)

    dataset2DDL = "id INT NOT NULL, btc_a STRING NOT NULL, cc_t STRING NOT NULL,cc_n STRING NOT NULL"

    financialDf = spark.read \
        .format("csv") \
        .option("header", "true") \
        .schema(dataset2DDL) \
        .option("mode", "FAILFAST") \
        .load(args.dataset2) \
        .drop('cc_n')

    Logger.info(financialDf)

    CustomerFinancialDf=FilteredCustomerDF.join(financialDf,FilteredCustomerDF.id==financialDf.id,"inner") \
        .drop(financialDf.id)

    colDict={'id': 'client_identifier',
            'btc_a' :'bitcoin_address',
            'cc_t' : 'credit_card_type'}

    processedDf=renameColumns(CustomerFinancialDf,colDict)

    processedDf.write \
        .format("csv") \
        .option("header","true") \
        .mode("overwrite") \
        .save('client_data')











