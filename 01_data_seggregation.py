import sys
from lib import dataReader, utils, dataManipulation, dataWriter

if __name__ == "__main__":

    if len(sys.argv) < 2:
        print("Please specify the environment")
        sys.exit(-1)
    
    job_run_env = sys.argv[1]

    print("creating spark session...")
    
    spark = utils.getSparkSession(job_run_env)

    print("Created Spark Session")

    raw_df = dataReader.initialRawReader(spark,job_run_env)

    df_with_unique_id = dataManipulation.createUniqueIdForMembers(raw_df)

    # print(df_with_unique_id.count())
    tableName = "raw_table"
    df_with_unique_id.createOrReplaceTempView(tableName)

    dataWriter.rawCustomerDataWriter(spark, tableName)
    dataWriter.rawLoansDataWriter(spark, tableName)
    dataWriter.rawLoansRepaymentDataWriter(spark, tableName)
    dataWriter.rawLoansDefaulterDataWriter(spark, tableName)

    spark.stop()
    print("Stopped Spark Session")
    



    
