"""spark utils"""

from pyspark.sql.types import StructType, StructField, StringType, DecimalType, \
    TimestampType, IntegerType, LongType, DateType, FloatType, BooleanType, DoubleType
from pyspark.sql import SparkSession
from src.utils.logger_builder import LoggerBuilder
from pyspark.sql.functions import col, lit

log = LoggerBuilder().build()
file_system_prefix = "s3://"


def fqn(db, table):
    """concatenate db and table name with dot"""
    return db + "." + table


def get_s3_data_location(bucket, table_name, target_key=None):
    """format s3 location where folder = table name and target key = prefix"""
    if target_key:
        return file_system_prefix + bucket + "/" + target_key + "/" + table_name
    return file_system_prefix + bucket + "/" + table_name


def table_exist(spark, db_name, table_name):
    """check if table exist in glue catalog"""
    if len([i for i in spark.catalog.listTables(db_name) if i.name == str(table_name)]) != 0:
        return True
    return False


def add_partitions(spark, table_fqn, partition_column, partition_value_list):
    """add partition list to the table metadata"""
    part_list = []
    for p in partition_value_list:
        part_list.append(f"PARTITION ({partition_column}='{p}')")
    query = """
            ALTER TABLE {}
            ADD IF NOT EXISTS {}
            """.format(table_fqn, ' '.join(part_list))

    spark.sql(query)


def save_or_overwrite_partitions(df, partition_column, table_location):
    """
    overwrite partitioned data
    """
    df.coalesce(3).write \
        .partitionBy(partition_column) \
        .mode("overwrite") \
        .format("parquet") \
        .save(table_location)


def save_as_new_partitioned_table(df, table_fqn, table_location,
                                  partition_column):
    """create new partitioned table"""

    df.write \
        .mode("overwrite") \
        .format("parquet") \
        .option("path", table_location) \
        .saveAsTable(table_fqn, partitionBy=partition_column)


def create_or_overwrite_partition_dynamic_table(spark=None,
                                                df=None,
                                                table_name=None,
                                                db_name=None,
                                                bucket=None,
                                                partition_column=None,
                                                partition_values=None,
                                                target_key=None):
    """
    Create a new partitioned table if not exist else overwrite the partitions.
    """
    spark.conf.set("spark.sql.hive.manageFilesourcePartitions", "true")

    table_fqn = fqn(db_name, table_name)
    table_location = get_s3_data_location(bucket, table_name, target_key)

    partition_value_list = partition_values.split(",")

    if len(partition_value_list) == 1 and partition_column not in df.columns:
        df = df.withColumn(partition_column, lit(partition_values))

    if table_exist(spark, db_name, table_name):
        spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

        log.info(f"{table_name} :: Table already exists. Will rewrite partition.")
        save_or_overwrite_partitions(df, partition_column, table_location)
        add_partitions(spark, table_fqn, partition_column, partition_value_list)

    else:
        spark.conf.set("spark.sql.sources.partitionOverwriteMode", "static")

        log.info(f"{table_name} : Table does not exist. Will create the table.")
        save_as_new_partitioned_table(df, table_fqn, table_location,
                                      partition_column)


def insert_data(df, db_name, table_name):
    """insert data to existing table"""
    df.coalesce(3).write.\
        mode("overwrite").\
        insertInto(f"{db_name}.{table_name}")


def create_or_overwrite_partition_static_table(spark=None,
                                               df=None,
                                               table_name=None,
                                               db_name=None,
                                               bucket=None,
                                               partition_column=None,
                                               target_key=None):
    """
    Create a new partitioned table if not exist else overwrite the partitions.
    """
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "static")

    table_fqn = fqn(db_name, table_name)
    table_location = get_s3_data_location(bucket, table_name, target_key)

    if table_exist(spark, db_name, table_name):
        spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
        log.info(f"{table_name} :: Table already exists. Will overwrite data.")
        insert_data(df, db_name, table_name)
    else:
        spark.conf.set("spark.sql.sources.partitionOverwriteMode", "static")
        log.info(f"{table_name} : Table does not exist. Will create the table.")
        save_as_new_partitioned_table(df, table_fqn, table_location,
                                      partition_column)


def map_dtypes(d_type):
    """map datatype from sql to spark"""
    if d_type == 'int' or d_type == 'integer':
        return IntegerType()
    elif d_type == 'bigint' or d_type == 'long':
        return LongType()
    elif d_type == 'varchar' or d_type == 'nvarchar' or d_type == 'text' or d_type == 'char':
        return StringType()
    elif d_type == 'datetime' or d_type == 'timestamp':
        return TimestampType()
    elif d_type == 'date':
        return DateType()
    elif d_type == 'float' or d_type == 'decimal':
        return FloatType()
    elif d_type == 'bit' or d_type == 'boolean':
        return BooleanType()
    elif d_type == 'double' or d_type == 'numeric':
        return DoubleType()
    elif d_type.startswith('double(') or d_type.startswith('decimal('):
        val = list(map(int, d_type.split('(')[1][:-1].split(',')))
        return DecimalType(val[0], val[1])
    else:
        return StringType()


def format_schema(schema_dict, default=None):
    """format schema from dict"""
    schema = StructType()
    for col in schema_dict:
        if default:
            d_type = StringType()
        else:
            d_type = map_dtypes(schema_dict[col])
        ss = StructField(col, d_type, True)
        schema.add(ss)
    return schema


def read_csv_spark(spark: SparkSession, input_path, custom_schema=None, sep="|"):
    """"
        This function takes current spark session, read data from path defined, separate it via | and return a DF
        @:param: spark : Current spark session
        @:param: input_path : Path to read data mostly it will be S3
        @:param: sep : record separator, by default it will be pipe
        @:param: header : Path contains header, by default its true
    """
    if custom_schema:
        df = spark.read \
            .option("sep", sep) \
            .option("header", "true") \
            .option("nullValue", "null") \
            .schema(custom_schema) \
            .option("quote", "\"") \
            .option("multiLine", "true") \
            .option("escape", "\"") \
            .csv(input_path)
    else:
        df = spark.read \
            .option("sep", sep) \
            .option("header", "true") \
            .option("nullValue", "null") \
            .option("quote", "\"") \
            .option("multiLine", "true") \
            .option("escape", "\"") \
            .csv(input_path)
    return df


def read_parquet(spark, src_path, custom_schema=None):
    """read parquet file"""
    if custom_schema:
        df = spark.read.schema(custom_schema).parquet(src_path)
    else:
        df = spark.read.parquet(src_path)
    return df


def save_data(df, table_location):
    """
    overwrite partitioned data
    """
    df.coalesce(3).write \
        .mode("overwrite") \
        .format("parquet") \
        .save(table_location)


def save_as_new_table(df, table_fqn, table_location):
    """create new partitioned table"""

    df.coalesce(3).write \
        .mode("overwrite") \
        .format("parquet") \
        .option("path", table_location) \
        .saveAsTable(table_fqn)


def create_or_overwrite_static_table(spark=None,
                                     df=None,
                                     table_name=None,
                                     db_name=None,
                                     bucket=None,
                                     target_key=None):
    """
    Create a new partitioned table if not exist else overwrite the partitions.
    """
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "static")

    table_fqn = fqn(db_name, table_name)
    table_location = get_s3_data_location(bucket, table_name, target_key)

    if table_exist(spark, db_name, table_name):
        spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
        log.info(f"{table_name} :: Table already exists. Will overwrite data.")
        save_data(df, table_location)
    else:
        spark.conf.set("spark.sql.sources.partitionOverwriteMode", "static")
        log.info(f"{table_name} : Table does not exist. Will create the table.")
        save_as_new_table(df, table_fqn, table_location)


def format_schema_select(schema_dict):
    """format schema from dict"""
    select_exp = []
    for col in schema_dict:
        select_exp.append(f"cast({col} as {schema_dict[col]}) as {col}")
    return select_exp
