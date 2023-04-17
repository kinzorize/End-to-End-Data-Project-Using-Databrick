from pyspark.sql.functions import monotonically_increasing_id
from pyspark.sql.functions import col
from pyspark.sql.functions import *
import urllib


# Define file type
file_type = "delta"
# Whether the file has a header
first_row_is_header = "true"
# Delimiter used in the file
delimiter = ","
# Read the CSV file to spark dataframe
aws_keys_df = spark.read.format(file_type)\
    .option("header", first_row_is_header)\
    .option("sep", delimiter)\
    .load("dbfs:/user/hive/warehouse/amazonproject_access_keys")


# Get the AWS access key and secret key from the spark dataframe
ACCESS_KEY = aws_keys_df.select('Access key ID').take(1)[0]['Access key ID']
SECRET_KEY = aws_keys_df.select('Secret access key').take(1)[
    0]['Secret access key']
# Encode the secrete key
ENCODED_SECRET_KEY = urllib.parse.quote(string=SECRET_KEY, safe="")


# AWS S3 bucket name
AWS_S3_BUCKET = "amazonkinzorize"
# Mount name for the bucket
MOUNT_NAME = "/mnt/amazonkinzorize"
# Source url
SOURCE_URL = "s3n://{0}:{1}@{2}".format(ACCESS_KEY,
                                        ENCODED_SECRET_KEY, AWS_S3_BUCKET)


# Mount the drive
dbutils.fs.mount(SOURCE_URL, MOUNT_NAME)


# Check if the AWS S3 bucket was mounted successfully
# It will display all the file in my s3 bucket
display(dbutils.fs.ls("/mnt/amazonkinzorize/amazon/"))


# File location and type
file_location = "/mnt/amazonkinzorize/amazon/Air_Conditioners.csv"
file_type = "csv"
# CSV options
infer_schema = "true"
first_row_is_header = "true"
delimiter = ","
# The applied options are for CSV files. For other file types, these will be ignored.
df = spark.read.format(file_type) \
    .option("inferSchema", infer_schema) \
    .option("header", first_row_is_header) \
    .option("sep", delimiter) \
    .load(file_location)
display(df)


# We want to add ID column to the dataset we have in our dataframe so
# it will be easy for us to query our data in snowflake

# read the dataset
df = spark.read.format("csv").option("header", True).load(
    "/mnt/amazonkinzorize/amazon/Air_Conditioners.csv")

# add a new column with unique ID number
df = df.withColumn("ID", monotonically_increasing_id())

# write the updated dataset to a new file
df.write.format("csv").option("header", True).save(
    "/mnt/amazonkinzorize/amazon/Updated_Air_Conditioners.csv")


# Set up the Snowflake credentials
options = {
    "sfURL": "https://app.snowflake.com/eu-north-1.aws/hc93404/w5CQYHRYj8kO#query",
    "sfUser": "",
    "sfPassword": "",
    "sfDatabase": "myamazondatabase",
    "sfSchema": "SCHEMA",
    "sfWarehouse": "COMPUTE_WH",
}


# Load the PySpark DataFrame
df = spark.read.format("<file_format>").options(** < file_format_options > ).load("<input_file_path>")

# Write the DataFrame to Snowflake


df = spark.read.format("csv").option("header", "true").load(
    "/mnt/amazonkinzorize/amazon/Updated_Air_Conditioners.csv").write.format("snowflake").options(**sfOptions).mode("append").option("dbtable", "transformed_table").save()

# query the dataframe in snowflake
df = spark.read \
    .format("snowflake") \
    .options(**options) \
    .option("query", "select * from transformed_table;") \
    .load()


display(df)
