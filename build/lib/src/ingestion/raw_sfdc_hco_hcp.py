from pyspark.sql import SparkSession
from pyspark.sql import Row
from src.services.spark import SparkService

# Initialize SparkSession
# spark = SparkSession.builder.appName("SparkDataframe").getOrCreate()
# spark.conf.set("spark.sql.legacy.parquet.int96RebaseModeInWrite", "LEGACY")
spark=SparkService.get_spark_session()
# Execute SQL query and create a DataFrame
df=spark.sql("select * from adl_enriched_gbl_cf_salesforce_archive.dim_account where RecordTypeId in( '012G00000016ciRIAQ','012G00000019HNJIA2')")
df1=spark.sql("select * from adl_enriched_gbl_cf_salesforce_archive.dim_address_vod__c where RecordTypeId in( '012G00000016ciRIAQ','012G00000019HNJIA2')")
# S3 bucket path
s3_bucket="s3://adl-base-customer-md-q5s69ag5wucw6xpfyke6a84xdtyxause1b-s3alias/raw/FS_SFDC_ACC_CUST/dim_account_test1"
s3_bucket1="s3://adl-base-customer-md-q5s69ag5wucw6xpfyke6a84xdtyxause1b-s3alias/raw/FS_SFDC_ACC_CUST/dim_address_vod__c_test1"
# Write DataFrame to S3 in Parquet format
#df.write.option("encoding", "UTF-8").mode("append").parquet(s3_bucket)
df.write.format("parquet").mode("overwrite").option("encoding", "UTF-8").save(s3_bucket)
df1.write.format("parquet").mode("overwrite").option("encoding", "UTF-8").save(s3_bucket1)
print("Data successfully written to dim_account")