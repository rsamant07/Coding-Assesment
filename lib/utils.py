import configparser
from pyspark.sql import SparkSession,DataFrame
from pyspark import SparkConf


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

def filterData(df: DataFrame,column: str,valuesList: list) -> DataFrame:
    """
          This function is provided to dynamically filter list of values from a given column in dataframe

          Args:
            df (DataFrame): dataframe which need to be filtered
            column (str) : columname on which filtering need to be done
            valuesList (list) : list of values to be included in the filter with or condition

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
                df (DataFrame): Input dataframe
                colDict (dict) : Dictionary with oldName as key and newName as Value

              Returns:
                    DataFrame : Dataframe with renamed column
              """
    print (df.columns)
    for old,new in colDict.items():
        df=df.withColumnRenamed(old,new)
    return df

def get_spark_app_config():
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




