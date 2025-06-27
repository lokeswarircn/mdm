from pyspark.sql import SparkSession

class Spark:

    @staticmethod
    def get_spark_session() -> SparkSession:
        spark = SparkSession \
        .builder \
        .appName("adl-customer-mdm") \
        .config("spark.sql.legacy.parquet.int96RebaseModeInWrite", "LEGACY") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain") \
        .getOrCreate()
        # spark.conf.set("key", "value")
        return spark
