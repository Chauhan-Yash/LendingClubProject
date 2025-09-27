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

    customer_df = dataReader.cleanedCustomerReader(spark)
    customer_df.createOrReplaceTempView("customers")
    bad_customer_df = dataManipulation.extract_bad_data(spark,"customers")


    loans_defaulters_delinq_df = dataReader.cleanedLoansDefDelinqReader(spark)
    loans_defaulters_delinq_df.createOrReplaceTempView("loans_defaulters_delinq")
    bad_loans_defaulters_delinq_df = dataManipulation.extract_bad_data(spark,"loans_defaulters_delinq")


    loans_defaulters_detail_rec_enq_df = dataReader.cleanedLoansDefDetailReader(spark)
    loans_defaulters_detail_rec_enq_df.createOrReplaceTempView("loans_defaulters_detail_rec_enq")
    bad_data_loans_def_detail_enq_df = dataManipulation.extract_bad_data(spark,"loans_defaulters_detail_rec_enq")


    # Creating a consolidated Bad Data df
    bad_df = bad_customer_df.select("member_id")\
        .union(bad_loans_defaulters_delinq_df.select("member_id"))\
        .union(bad_data_loans_def_detail_enq_df.select("member_id"))
    
    bad_data_df = bad_df.distinct()

    bad_data_df.createOrReplaceTempView("bad_data_customer")

    customer_without_bad_data_df = dataManipulation.remove_bad_data(spark,"bad_data_customer","customers")
    file_path = "../data/cleaned_new/customers_parquet"
    dataWriter.new_cleaned_writer(customer_without_bad_data_df,file_path)

    loans_defaulters_delinq_without_bad_data_df = dataManipulation.\
        remove_bad_data(spark,"bad_data_customer","loans_defaulters_delinq")
    file_path = "../data/cleaned_new/loans_defaulters_delinq_parquet"
    dataWriter.new_cleaned_writer(loans_defaulters_delinq_without_bad_data_df,file_path)

    loans_defaulters_detail_rec_enq_without_bad_data_df = dataManipulation.\
        remove_bad_data(spark,"bad_data_customer","loans_defaulters_detail_rec_enq")
    file_path = "../data/cleaned_new/loans_defaulters_detail_rec_enq_parquet"
    dataWriter.new_cleaned_writer(loans_defaulters_detail_rec_enq_without_bad_data_df,file_path)


    spark.stop()
    print("Stopped Spark Session")