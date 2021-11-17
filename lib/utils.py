import configparser
from pyspark.sql import SparkSession,DataFrame
from pyspark import SparkConf
from pyspark.sql.types import StructType

def loadConfig(Section=None):
    """
       Load Configuration from conf/config.ini file for dynamic parameters . This Function can Provide
       Section Specific parameters or all Sections depending on input parameter
       Args:
        Section : Optional Section Name for which configuration is needed
       Returns:
        conf (configparser.SectionProxy): Configuration in Dictionary type object
       """
    Configuration = configparser.ConfigParser()
    Configuration.read("conf/config.ini")

    if Section is None:
        conf = Configuration
    else:
        conf = Configuration[Section]
    print(" conf type",type(conf))
    return conf


def loadCsvDataset(spark: SparkSession,filepath: str,schema: StructType) ->DataFrame:
    """
        This function loads the CSV into Dataframe as per the schema and file path provided
        Args:
            df : dataframe which need to be filtered
            filepath : path for folder where output needs to be written
        Returns:
            DataFrame : DataFrame created from CSV

        """
    return spark.read \
            .format("csv") \
            .option("header", "true") \
            .schema(schema) \
            .option("mode", "FAILFAST") \
            .load(filepath)


def writeCsv(df: DataFrame,filepath: str):
    """
        This function writes the provided dataframe into csv format under given path
        Args:
            df : dataframe which need to be filtered
            filepath : path for folder where output needs to be written
        Returns:
    """
    return df.write \
        .format("csv") \
        .option("header","true") \
        .mode("overwrite") \
        .save('client_data')


def filterData(df: DataFrame,column: str,valuesList: list) -> DataFrame:
    """
          This function will dynamically filter list of values from a given column in dataframe
          Args:
            df : dataframe which need to be filtered
            column : columname on which filtering need to be done
            valuesList : list of values to be included in the filter with or condition
          Returns:
                DataFrame : Dataframe with filtered data
          """
    filterCondition=[]
    print (df.columns)
    for value in valuesList:
        filterCondition.append(f'{column} == "{value}"')
    return df.filter(' OR '.join(filterCondition))

def renameColumns(df: DataFrame,colDict: dict) -> DataFrame:
    """
        This function is provided to rename column name in  given dataframe
        as per the column mapping provided in dictionary input
        Args:
            df : Input dataframe
            colDict  : Dictionary with oldName as key and newName as Value
        Returns:
            DataFrame : Dataframe with renamed column
    """
    print (df.columns)
    for old,new in colDict.items():
        df=df.withColumnRenamed(old,new)
    return df

def get_spark_app_config()-> SparkConf:
    """
        This function will initialize SparkCong Object as per the parameter value in Configuraiton File

            Returns:
               SparkConf : SparkConf object
    """
    spark_conf = SparkConf()
    config = loadConfig("SPARK_APP_CONFIGS")
    for (key, val) in config.items():
        spark_conf.set(key, val)
        print(key,val)
    return spark_conf




