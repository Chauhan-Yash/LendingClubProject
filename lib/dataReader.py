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
        .option("inferSchema","true")\
        .option("header","true")\
        .load(filePath)