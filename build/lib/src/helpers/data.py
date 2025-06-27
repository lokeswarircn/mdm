import requests
from typing import Optional, List, Dict
from pyspark.sql import DataFrame as SparkDataFrame
from src.helpers.utils import s3
from src.utils.params import Params

from src.services.spark import Spark

class Data:

    spark=Spark.get_spark_session()

    @classmethod
    def _read_config(cls) -> dict:
        import sys, yaml, boto3
        from botocore.exceptions import ClientError
        try:
            s3_client = boto3.client('s3')
            bucket, key = s3.parse_s3_uri(Params(sys.argv).get('configpath'))
            
            response = s3_client.get_object(Bucket=bucket, Key=key)
            file_content = response['Body'].read().decode('utf-8')
            
            return  yaml.safe_load(file_content)
        
        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code == 'NoSuchBucket' or error_code == 'NoSuchKey':
                raise Exception(f"Config file not found in S3: {str(e)}")
            else:
                raise Exception(f"Error accessing S3: {str(e)}")
        except Exception as e:
            raise Exception(f"Failed to load config: {str(e)}")
        

    @classmethod
    def _read_athena(cls, query:str):
         return cls.spark.sql(query)
    @classmethod
    def _read_csv(cls, rel_path:str, **options) ->  SparkDataFrame:
        return cls.spark.read.option("header", "true").option("inferSchema", "true").csv(rel_path)
   
    @classmethod
    def _read_parquet(cls, path:str, **options) ->  SparkDataFrame:
        return cls.spark.read.parquet(path)
    
    @classmethod
    def api(cls, url, **options) -> List[Dict]:
        return requests.get(url).json()

    @classmethod
    def read(cls, source_type:str, path:str =None, source_query:str=None, file_format:str=None):
        if source_type == 'athena':
            return cls._read_athena(source_query)
        if file_format=='file':
            return cls._read_parquet(path)
        
    @classmethod
    def write(cls, target_type, df, path, write_mode='overwrite', **options):
        if target_type == 'parquet':
            df.write.format(target_type).mode(write_mode).option("encoding", "UTF-8").save(path)
        if target_type == 'json':
            df.repartition(5).write.mode(write_mode).json(path)