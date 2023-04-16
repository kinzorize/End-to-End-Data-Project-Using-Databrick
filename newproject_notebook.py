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
display(dbutils.fs.ls("/mnt/amazonkinzorize/amazon/"))
