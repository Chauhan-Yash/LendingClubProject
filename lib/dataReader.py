from lib import configReader

def initialRawReader(spark,env):
    conf = configReader.get_app_config(env)
    filePath = conf["data.file.path"]
    return spark.read.format("csv")\
        .option("inferSchema","true")\
        .option("header","true")\
        .load(filePath)

def rawCustomerReader(spark):
    customer_schema = """
        member_id string, emp_title string, emp_length string, home_ownership string, annual_inc float, 
        addr_state string, zip_code string, country string, grade string, sub_grade string, 
        verification_status string, tot_hi_cred_lim float, application_type string, 
        annual_inc_joint float, verification_status_joint string
    """
    filePath = "../data/raw/customers_data_csv"
    return spark.read.format("csv")\
        .schema(customer_schema)\
        .option("header","true")\
        .load(filePath)

def rawLoansReader(spark):
    loans_schema = """
        loan_id string, member_id string, loan_amount float, funded_amount float, loan_term_months string,
        interest_rate float, monthly_installment float, issue_date string, loan_status string, loan_purpose string,
        loan_title string
    """
    filePath = "../data/raw/loans_data_csv"
    return spark.read.format("csv")\
        .schema(loans_schema)\
        .option("header","true")\
        .load(filePath)

def rawLoansRepaymentsReader(spark):
    loans_repay_schema = """
        loan_id string, total_principal_received float, total_interest_received float, total_late_fee_received float,
        total_payment_received float, last_payment_amount float, last_payment_date string, next_payment_date string
    """
    filePath = "../data/raw/loans_repayments_csv"
    return spark.read.format("csv")\
        .schema(loans_repay_schema)\
        .option("header","true")\
        .load(filePath)

def rawLoansDefaulterReader(spark):
    loan_defaulters_schema = """
        member_id string, delinq_2yrs float, delinq_amnt float, pub_rec float, pub_rec_bankruptcies float,
        inq_last_6mths float, total_rec_late_fee float, mths_since_last_delinq float, mths_since_last_record float"""
    
    filePath = "../data/raw/loans_defaulters_csv"
    return spark.read.format("csv")\
        .schema(loan_defaulters_schema)\
        .option("header","true")\
        .load(filePath)
