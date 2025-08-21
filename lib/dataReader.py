from lib import configReader

def initialRawReader(spark,env):
    conf = configReader.get_app_config(env)
    filePath = conf["data.file.path"]
    return spark.read.format("csv")\
        .option("inferSchema","true")\
        .option("header","true")\
        .load(filePath)
