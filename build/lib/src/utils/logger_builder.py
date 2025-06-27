import yaml
import boto3
import sys
from dataclasses import dataclass
from src.helpers.dataclass import Pipeline, Athena, File

from botocore.exceptions import ClientError
from src.helpers.utils import s3
from src.utils.params import Params

@dataclass
class YamlParser:  
    source: str
    entity: str
    layer: str

    def extract(self):
        return self._read_config_from_yaml()
    
    # def _get_src_tgt_info(self):
    #     config_dict:dict = self._read_config_from_yaml()
    #     resource_info = config_dict.get(self.source).get(self.entity).get(self.layer)
    #     src_type, src_query = self._extract_source_info(resource_info.get('source'))
    #     tgt_bucket, tgt_prefix = self._extract_target_info(resource_info.get('target'))
    #     return src_type, src_query, tgt_bucket, tgt_prefix

    def _extract_source_info(self,source_info):
        if source_info.get('type') == 'athena':
            return source_info.get('type'), source_info.get('query')
        
    def _extract_target_info(self,target_info):
        if target_info.get('type') == 'file':
            return target_info.get('bucket'), target_info.get('prefix')
        
    def _read_config_from_yaml(self):
        """Read config file from S3"""
        try:
            s3_client = boto3.client('s3')
            bucket, key = s3.parse_s3_uri(Params(sys.argv).get('configpath'))
            
            response = s3_client.get_object(Bucket=bucket, Key=key)
            file_content = response['Body'].read().decode('utf-8')
            config_data = yaml.safe_load(file_content)
            return Pipeline(**config_data)
        
        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code == 'NoSuchBucket' or error_code == 'NoSuchKey':
                raise Exception(f"Config file not found in S3: {str(e)}")
            else:
                raise Exception(f"Error accessing S3: {str(e)}")
        except Exception as e:
            raise Exception(f"Failed to load config: {str(e)}")
        
    def _create_pipeline_from_config(config: dict) -> Pipeline:
        pipeline = Pipeline()
        
        for resource in config.get('dnb', {}).get('hco', {}).get('raw', {}).get('resources', []):
            if resource.get('source', {}).get('type') == 'athena':
                pipeline.dnb.hco.raw.athena = Athena(**resource['source'])
                pipeline.dnb.hco.raw.file = File(**resource['target'])
        
        return pipeline