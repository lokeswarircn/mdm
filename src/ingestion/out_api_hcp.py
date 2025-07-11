
import sys
from src.utils.params import Params
args = Params(sys.argv)
print(f"sys.argv :          {sys.argv}")
print(f"env :               {args.get('env','no env')}")
print(f"test_path :               {args.get('test_path','no test_path')}")
print(f"test_path_alias :               {args.get('test_path_alias','no test_path_alias')}")

# print(f"source_path :       {args.get('source_path','Oops')}")
# print(f"target_path :       {args.get('target_path','Oops')}")
# print(f"config_path :       {args.get('config_path','Oops')}")
# print(f"target_database :   {args.get('target_database','Oops')}")
# print(f"dynamodb_table :    {args.get('dynamodb_table','Oops')}")
# print(f"secret_name :       {args.get('secret_name','Oops')}")
# print(f"envtest: {args.get('envtest','NOT SENT FROM SFN ARGS')}")
# print(f"ENV: {args.get('ENV','NOT SENT FROM SFN ARGS')}")
# print(f"ENV: {args.get('ENV','NOT SENT FROM SFN ARGS')}")

# print(f"SOURCE_PATH: {args.get('SOURCE_PATH','NOT SENT FROM SFN ARGS')}")
# print(f"TARGET_PATH: {args.get('TARGET_PATH','NOT SENT FROM SFN ARGS')}")
# print(f"CONFIG_PATH: {args.get('CONFIG_PATH','NOT SENT FROM SFN ARGS')}")
# print(f"TARGET_DATABASE: {args.get('TARGET_DATABASE','NOT SENT FROM SFN ARGS')}")
# print(f"SECRET_KEY: {args.get('SECRET_KEY','NOT SENT FROM SFN ARGS')}")
# print(f"DYNAMODB_TABLE: {args.get('DYNAMODB_TABLE','NOT SENT FROM SFN ARGS')}")
# print(f"TABLE_LIST: {args.get('TABLE_LIST','NOT SENT FROM SFN ARGS')}")
# print(f"SECRET_NAME: {args.get('SECRET_NAME','NOT SENT FROM SFN ARGS')}")

# import requests
# import json
# import time
# import logging
# import boto3

# LOG_FORMAT = '%(asctime)s %(levelname)s - %(module)s - %(funcName)s - %(message)s'
# logging.basicConfig(format=LOG_FORMAT, level=logging.INFO)
# logger = logging.getLogger(__name__)

# class ExportApi:
#     def __init__(self, client_id, client_secret, entity):
#         self.s3 = boto3.client('s3')
#         self.trigger_load_url = "https://dev.api.alcon.com/reltio-exp-sys/api/triggerLoad"
#         self.status_url = "https://dev.api.alcon.com/reltio-exp-sys/api/loadStatus"
#         self.client_id = client_id
#         self.client_secret = client_secret
#         self.entity= entity
#         self.headers = {
#             "Content-Type": "application/json",
#             "client_id": self.client_id,
#             "client_secret": self.client_secret
#         }
#         self.raw_bucket = "adl-base-customer-md-q5s69ag5wucw6xpfyke6a84xdtyxause1b-s3alias"
#         self.reltio_prefix_man = f"raw/Reltio/{self.entity}/manifest.json"
#         self.reltio_prefix_hco = f"raw/Reltio/{self.entity}/"

#     def step1_trigger_load(self):
#         payload = {
#             "loadType": "export",
#             "entityType": self.entity
#         }
#         response = requests.post(self.trigger_load_url, headers=self.headers, data=json.dumps(payload))
#         logger.info(f"response:{response}")
#         task_id = response.json().get("taskIds")[0]
#         # return response
#         logger.info(f"task_id:{task_id}")
#         # time.sleep(10)
#         self.step2_check_status(task_id)

#     def step2_check_status(self, task_id):
#         load_url = f"{self.status_url}?loadType=export&taskId={task_id}"
#         logger.info(f"load_url:{load_url}")
#         while True:
#             response = requests.get(load_url, headers=self.headers)
#             if response.json().get('status') in ['COMPLETED', 'COMPLETED_WITH_ERRORS']:
#                     break
#             time.sleep(10)  # wait for 10 seconds before polling again
#         self.step3_trigger_load_manifest()

#     def step3_trigger_load_manifest(self):
#         payload = {
#             "loadType": "export",
#             "entityType": self.entity,
#             "copyFile": "true",
#             "objectKey": "manifest.json"
#         }
#         manifest_response = requests.post(self.trigger_load_url, headers=self.headers, data=json.dumps(payload))
#         time.sleep(5)
#         manifest_content = self.s3.get_object(Bucket=self.raw_bucket, Key=self.reltio_prefix_man)['Body'].read().decode('utf-8')
#         logger.critical(f"{json.loads(manifest_content)}")
#         return self.step4_download_files(json.loads(manifest_content).get('files'))
    
#     def step4_download_files(self,json_zip_files):
#         logger.info(f"json_zip_files:{json_zip_files}")
#         for copyzipfile in json_zip_files:
#             logger.info(f"copyzipfile:{copyzipfile}")
#             payload = {
#             "loadType": "export",
#             "entityType": self.entity,
#             "copyFile": "true",
#             "objectKey": str(copyzipfile)
#             }
#             requests.post(self.trigger_load_url, headers=self.headers, data=json.dumps(payload))
#             time.sleep(1)

            
#         import gzip
#         import io

#         response = self.s3.list_objects_v2(Bucket=self.raw_bucket, Prefix=self.reltio_prefix_hco)
#         logger.info(f"response: {response}")
        
#         if 'Contents' in response:
#             json_files = [obj['Key'] for obj in response['Contents'] if obj['Key'].endswith('.gz')]
#             logger.critical(f"json_files zipped: {json_files}")
#             for gz_file in json_files:
#                 try:
#                     gz_obj = self.s3.get_object(Bucket=self.raw_bucket, Key=gz_file)
#                     with gzip.GzipFile(fileobj=io.BytesIO(gz_obj['Body'].read()), mode='rb') as gz_data:
#                         json_content = gz_data.read().decode('utf-8')
#                     unzipped_key = gz_file.replace('.json.gz', '.json').replace(f'/{self.entity}/', f'/{self.entity}/unzipped/')
#                     self.s3.put_object( Bucket=self.raw_bucket, Key=unzipped_key, Body=json_content, ContentType='application/json')
#                     logger.info(f"Successfully unzipped and uploaded: {unzipped_key}")
#                 except Exception as e:
#                     logger.error(f"Error processing file {gz_file}: {str(e)}")
#                     continue
#             self.step5_delete_source_files(json_zip_files)
            
#         return None
#     def step5_delete_source_files(self,json_zip_files):
#         json_zip_files.append('manifest.json')
#         payload = {
#             "loadType": "export",
#             "entityType": self.entity,
#             "deleteFiles": json_zip_files
#             }
#         delete_respopnse=requests.post(self.trigger_load_url, headers=self.headers, data=json.dumps(payload))
#         logger.info(f"delete_respopnse:{delete_respopnse}")


# api_caller = ExportApi(
#     client_id="a9bfd4b0970e4ab1a38c80d70adf94e1",
#     client_secret="33aa6063a6d944fDA6BdbBdD431d4c25",
#     entity='HCO'
# )

# api_caller.step1_trigger_load()
                                                
