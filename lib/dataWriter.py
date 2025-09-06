def rawCustomerDataWriter(spark,tableName):
    df = spark.sql("""
        select 
              name_sha2 as member_id,emp_title,emp_length,home_ownership,annual_inc,addr_state,
              zip_code,'USA' as country,grade,sub_grade,verification_status,tot_hi_cred_lim,
              application_type,annual_inc_joint,verification_status_joint 
        from {}
    """.format(tableName))

    df.repartition(1).write\
        .option("header","true")\
        .format("csv")\
        .mode("overwrite")\
        .save("../data/raw/customers_data_csv")
    
def rawLoansDataWriter(spark,tableName):
    df = spark.sql("""
        select 
              id as loan_id, name_sha2 as member_id,loan_amnt,funded_amnt,term,int_rate,
              installment,issue_d,loan_status,purpose,title 
        from {}
    """.format(tableName))

    df.repartition(1).write\
        .option("header","true")\
        .format("csv")\
        .mode("overwrite")\
        .save("../data/raw/loans_data_csv")

def rawLoansRepaymentDataWriter(spark,tableName):
    df = spark.sql("""
        select 
              id as loan_id,total_rec_prncp,total_rec_int,total_rec_late_fee,total_pymnt,
              last_pymnt_amnt,last_pymnt_d,next_pymnt_d
        from {}
    """.format(tableName))

    df.repartition(1).write\
        .option("header","true")\
        .format("csv")\
        .mode("overwrite")\
        .save("../data/raw/loans_repayments_csv")
    
def rawLoansDefaulterDataWriter(spark,tableName):
    df = spark.sql("""
        select 
              name_sha2 as member_id,delinq_2yrs,delinq_amnt,pub_rec,pub_rec_bankruptcies,
              inq_last_6mths,total_rec_late_fee,mths_since_last_delinq,mths_since_last_record
        from {}
    """.format(tableName))

    df.repartition(1).write\
        .option("header","true")\
        .format("csv")\
        .mode("overwrite")\
        .save("../data/raw/loans_defaulters_csv")

def cleanedCustomersDfWriter(df):
    df.write.format("parquet")\
        .mode("overwrite")\
        .save("../data/cleaned/customers_parquet")
    
def cleanedLoansDfWriter(df):
    df.write.format("parquet")\
        .mode("overwrite")\
        .save("../data/cleaned/loans_parquet")

def cleanedLoansRepaymentsDfWriter(df):
    df.write.format("parquet")\
        .mode("overwrite")\
        .save("../data/cleaned/loans_repayments_parquet")
    
def cleanedLoansDefaultersDfWriter(loans_def_delinq_df,loans_def_records_enq_df):
    loans_def_delinq_df.write.format("parquet")\
        .mode("overwrite")\
        .save("../data/cleaned/loans_defaulter_delinq_parquet")
    loans_def_records_enq_df.write.format("parquet")\
        .mode("overwrite")\
        .save("../data/cleaned/loans_defaulters_records_enq_parquet")
    

        