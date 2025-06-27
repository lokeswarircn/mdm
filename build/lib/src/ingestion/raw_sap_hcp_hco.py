from pyspark.sql import SparkSession
from pyspark.sql import Row


# Initialize SparkSession
spark = SparkSession.builder.appName("SparkDataframe").getOrCreate()

# Execute SQL query and create a DataFrame
df = spark.sql("select * FROM adl_enriched_gbl_cf_reference.dim_customer where Customer_Account_Group in('8C05', '8C01', '8C02', '8C03' ,'8C04' , '8C06', '8C07', '8C08','8C0A', '8C13')")

# S3 bucket path
s3_bucket = "s3://adl-base-customer-md-q5s69ag5wucw6xpfyke6a84xdtyxause1b-s3alias/raw/FS_SAP_DC_CUST/dim_customer1"
# Write DataFrame to S3 in Parquet format
df.write.option("encoding", "UTF-8").mode("overwrite").parquet(s3_bucket)

print("Data successfully written to dim_customer")