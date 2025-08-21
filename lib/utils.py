from pyspark.sql import SparkSession
from lib.configReader import get_pyspark_config

def getSparkSession(env):
    if env == "LOCAL":
        return SparkSession.builder\
            .config(conf= get_pyspark_config(env))\
            .master("local[2]")\
            .getOrCreate()
    else:
        return SparkSession.builder\
            .config(conf= get_pyspark_config(env))\
            .enableHiveSupport()\
            .getOrCreate()
        
