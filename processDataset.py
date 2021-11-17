import argparse
from pyspark.sql import SparkSession,DataFrame
from pyspark.sql.functions import expr
from pyspark.sql.types import StructType, StructField, DateType, StringType, IntegerType
from lib.utils import *
from lib.logger import get_logger,logLevelDict

Logger = get_logger('Logging')

def processCustomerInfo(spark: SparkSession,args) -> DataFrame:
    """
        This function reads the csv files for the given datsets removes the PII . It handles
        the transformation logic provided for
        Args:
            Spark : Spark Session for the  application
            args  : Variable with all Parameters passed to program
        Returns:
            Datafram: Procssed dataframe with the  marketing required information
    """


    ## Load Dataset for customer Information
    dataset1DDL = StructType([
        StructField("id", IntegerType(), False),
        StructField("first_name", StringType(), False),
        StructField("last_name", StringType(), False),
        StructField("email", StringType(), False),
        StructField("country", StringType(), False)
    ])

    customerDF = loadCsvDataset(spark, args.dataset1, dataset1DDL) \
        .drop('first_name') \
        .drop('last_name')

    Logger.info(f"Customer Information Dataset loaded with schema {customerDF.schema}", )

    FilteredCustomerDF = filterData(customerDF, 'country', args.countrylist.split(','))

    Logger.info(FilteredCustomerDF)

    # Load Dataset for customer Financial Information
    dataset2DDL = dataset1DDL = StructType([
        StructField("id", IntegerType(), False),
        StructField("btc_a", StringType(), False),
        StructField("cc_t", StringType(), False),
        StructField("cc_n", StringType(), False)
    ])

    financialDf = loadCsvDataset(spark, args.dataset2, dataset2DDL) \
        .drop('cc_n')

    Logger.info(f"Customer Financial Information Dataset loaded with schema : {financialDf.schema} ")

    CustomerFinancialDf = FilteredCustomerDF.join(financialDf, FilteredCustomerDF.id == financialDf.id, "inner") \
        .drop(financialDf.id)

    colDict = {'id': 'client_identifier',
               'btc_a': 'bitcoin_address',
               'cc_t': 'credit_card_type'}

    processedDf = renameColumns(CustomerFinancialDf, colDict)

    Logger.info(f"Final Output to be written in csv with format schema : {processedDf.schema} ")

    return processedDf


if __name__ == "__main__":
    #Parse Command Arguments
    parser = argparse.ArgumentParser()

    parser.add_argument('--dataset1', help="Customer Dataset Path",required=True)
    parser.add_argument('--dataset2', help="Financial Dataset Path", required=True)
    parser.add_argument('--countrylist', help="Countries list for filter", required=True)

    args = parser.parse_args()
    #Load Spark Config from Configuration File
    sparkConf=get_spark_app_config()

    spark = SparkSession \
        .builder \
        .config(conf=sparkConf) \
        .getOrCreate()

    Logger.info(args)

    processedDf=processCustomerInfo(spark,args)
    #write the output to provided path
    writeCsv(processedDf,'client_data')









    processedDf.write \
        .format("csv") \
        .option("header","true") \
        .mode("overwrite") \
        .save('client_data')











