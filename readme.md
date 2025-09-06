# Git Steps:
    1. Created a local project folder.
    2. git init
    3. modified the branch name (git branch -m main)
    4. added remote repo (git remote add origin "https://url")


# Details of the Project : 

## Setup : 
1. for virtual environment we are using pipenv.
2. start with "pipenv install pyspark"
3. it will automatically create the virtual env with all the necessary dependencies
4. to start the venv , run "pipenv shell" 

## Dataset :

we are using https://www.kaggle.com/datasets/wordsforthewise/lending-club?resource=download dataset.

From the above dataset we have used "accepted_2007_to_2018Q4.csv"

since it is big dataset we have not included in this git repo.
Therefore adjust the path as per your need.


# configReader.py : 
It helps to read all the configs specific to diff environment as specified to user. eg : "LOCAL"

# utils.py : 
it will help us in creating the spark-session for all the files based on the env.

# dataReader.py : 
It will contain all the DF reading snippets

# dataManipulation.py : 
All the transformation logic will be written here 

# dataWriter.py :
It will help writing data back to the disk

Above mentioned are all the module which we'll be using for writing aur pyspark code.
I have mentioned each and every files name in sequential order in which they are supposed to be executed as an individual pipeline one after another.

# 01_data_seggregation :
In this we have observerd the data and seggregated it in 4 major stream which we'll use for further processing.

# 02_cleaning_customers_data : 
 create a dataframe with proper datatypes
 Rename a few columns
 insert a new column named as ingestion date(current time)
 Remove complete duplicate rows
 Remove the rows where annual_income is null
 convert emp_length to integer
 we need to replace all the nulls in emp_length column with average of this column
 Clean the address_state(it should be 2 characters only),replace all others with NA

# 03_cleaning_loans_data : 
    create a dataframe with proper datatypes and names
    insert a new column named as ingestion date(current time)
    Dropping the rows which has null values in the mentioned columns
    convert loan_term_months to integer
    Clean the loans_purpose column

# 04_cleaning_loans_repayments_data:
    insert a new column named as ingestion date(current time)
    drop rows having these columns as nulls
    check total_payment_received column
    filter out rows having total_payment_received != 0.0

# 05_cleaning_loans_defaulter_data:
    casted delinq_2yrs to integer, and filled NA with 0
    Make two dfs out of it 
    1 for defaulters_delinq
    1 for defaulters_recored_enquiry
