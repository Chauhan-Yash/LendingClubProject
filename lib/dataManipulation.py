from pyspark.sql.functions import *

def createUniqueIdForMembers(raw_df):
    return raw_df.withColumn("name_sha2",sha2(concat_ws("||", *["emp_title", "emp_length", "home_ownership", 
                                                                "annual_inc", "zip_code", "addr_state", "grade",
                                                                  "sub_grade","verification_status"]), 256))

def cleanCustomerDf(df):
    # rename few columns
    customer_df_renamed = df.withColumnRenamed("annual_inc", "annual_income") \
        .withColumnRenamed("addr_state", "address_state") \
        .withColumnRenamed("zip_code", "address_zipcode") \
        .withColumnRenamed("country", "address_country") \
        .withColumnRenamed("tot_hi_cred_lim", "total_high_credit_limit") \
        .withColumnRenamed("annual_inc_joint", "join_annual_income")
    
    # insert a new column named as ingestion date(current time)
    customers_df_ingestd = customer_df_renamed.withColumn("ingest_date",current_timestamp())

    # Remove complete duplicate rows
    customers_distinct = customers_df_ingestd.distinct()

    # Remove the rows where annual_income is null
    customers_income_filtered = customers_distinct.filter("annual_income is not null")

    # convert emp_length to integer
    customers_emplength_cleaned = customers_income_filtered\
        .withColumn("emp_length", regexp_replace(col("emp_length"), "(\D)", ""))
    customers_emplength_casted = customers_emplength_cleaned\
        .withColumn("emp_length", customers_emplength_cleaned.emp_length.try_cast("int"))
    
    # we need to replace all the nulls in emp_length column with average of this column
    avg_emp_length = customers_emplength_casted.select(floor(avg("emp_length"))).collect()
    avg_emp_duration = avg_emp_length[0][0]
    customers_emplength_replaced = customers_emplength_casted.fillna(avg_emp_duration, subset=["emp_length"])

    # Clean the address_state(it should be 2 characters only),replace all others with NA
    customers_state_cleaned = customers_emplength_replaced\
        .withColumn("address_state",
                    when(length(col("address_state")) > 2, "NA").otherwise(col("address_state"))
                    )
    
    print(customers_state_cleaned.count())
    
    return customers_state_cleaned





