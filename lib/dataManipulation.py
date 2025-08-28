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

def cleanLoansDf(df) :
    # insert a new column named as ingestion date(current time)
    loans_df_ingestd = df.withColumn("ingest_date", current_timestamp())

    # Dropping the rows which has null values in the mentioned columns
    columns_to_check = [
        "loan_amount", "funded_amount", "loan_term_months", "interest_rate", "monthly_installment",
        "issue_date", "loan_status", "loan_purpose"
    ]
    loans_filtered_df = loans_df_ingestd.na.drop(subset=columns_to_check)

    # convert loan_term_months to integer
    loans_term_modified_df = loans_filtered_df\
        .withColumn("loan_term_months", (regexp_replace(col("loan_term_months"), " months", "") \
            .cast("int") / 12) \
            .cast("int")) \
        .withColumnRenamed("loan_term_months","loan_term_years")
    
    # Clean the loans_purpose column
    loan_purpose_lookup = [
        "debt_consolidation", "credit_card", "home_improvement", "other", "major_purchase", "medical",
        "small_business", "car", "vacation", "moving", "house", "wedding", "renewable_energy", "educational"
    ]
    loans_purpose_modified = loans_term_modified_df\
        .withColumn(
            "loan_purpose", 
            when(col("loan_purpose").isin(loan_purpose_lookup), col("loan_purpose")).otherwise("other")
            )
    
    return loans_purpose_modified

def cleanLoansRepaymentsDf(df):
    # insert a new column named as ingestion date(current time)
    loans_repay_df_ingestd = df.withColumn("ingest_date", current_timestamp())

    # drop rows having these columns as nulls
    columns_to_check = [
        "total_principal_received", "total_interest_received", "total_late_fee_received", 
        "total_payment_received", "last_payment_amount"
    ]
    loans_repay_filtered_df = loans_repay_df_ingestd.na.drop(subset=columns_to_check)


    # check total_payment_received column
    loans_payments_fixed_df = loans_repay_filtered_df.withColumn(
        "total_payment_received",
        when(
            (col("total_principal_received") != 0.0) &
            (col("total_payment_received") == 0.0),
            col("total_principal_received") + col("total_interest_received") + col("total_late_fee_received")
        ).otherwise(col("total_payment_received"))
    )

    # filter out rows having total_payment_received != 0.0
    loans_payments_fixed2_df = loans_payments_fixed_df.filter("total_payment_received != 0.0")

    loans_payments_ldate_fixed_df = loans_payments_fixed2_df.withColumn(
        "last_payment_date",
        when(
            (col("last_payment_date") == '0.0'),
            None
            ).otherwise(col("last_payment_date"))
    )

    loans_payments_ndate_fixed_df = loans_payments_ldate_fixed_df.withColumn(
        "next_payment_date",
        when(
            (col("next_payment_date") == '0.0'),
            None
            ).otherwise(col("next_payment_date"))
    )

    return loans_payments_ndate_fixed_df





