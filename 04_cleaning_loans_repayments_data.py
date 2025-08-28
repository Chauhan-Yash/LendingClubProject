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

    loans_repayments_raw_df = dataReader.rawLoansRepaymentsReader(spark)

    resultDf = dataManipulation.cleanLoansRepaymentsDf(loans_repayments_raw_df)


    dataWriter.cleanedLoansRepaymentsDfWriter(resultDf)

    spark.stop()
    print("Stopped Spark Session")
    



    