import requests
import json
import boto3
import logging
import time

# Configure logging
LOG_FORMAT = '%(asctime)s %(levelname)s - %(module)s - %(funcName)s - %(message)s'
logging.basicConfig(format=LOG_FORMAT, level=logging.INFO)
logger = logging.getLogger(__name__)

class DataLoader:
    def __init__(self, client_id, client_secret, s3_bucket, s3_prefix, entity):

        # Authentication and configuration
        self.client_id = client_id
        self.client_secret = client_secret
        self.s3_bucket = s3_bucket
        self.s3_prefix = s3_prefix
        self.entity = entity
        
        # API endpoints
        self.load_url = "https://dev.api.alcon.com/reltio-exp-sys/api/triggerLoad"
        self.status_url = "https://dev.api.alcon.com/reltio-exp-sys/api/loadStatus"
        self.error_url = "https://dev.api.alcon.com/reltio-exp-sys/api/loadError"
       
        # Request headers
        self.headers = {
            "Content-Type": "application/json",
            "client_id": self.client_id,
            "client_secret": self.client_secret
        }
       


        # State tracking
        self.job_id_list = []
        self.poll_results = []
        self.failed_job_id_list = []

        # Initialize AWS S3 client
        self.s3 = boto3.client('s3')

    def list_s3_objects(self):
        logger.info("start")
        response = self.s3.list_objects_v2(Bucket=self.s3_bucket, Prefix=self.s3_prefix)
        logger.info(f"response: {response}")
        if 'Contents' in response:
            json_files = [obj['Key'] for obj in response['Contents'] if obj['Key'].endswith('.json')]
        logger.info(f"json_files: {json_files}")
        return json_files

    def detect_files(self):
        logger.info("func start")
        json_files = self.list_s3_objects()
        logger.info(f"json_files: {json_files}")

        for file in json_files:
            logger.info(f"file: {file}")
            self.file=file
            self.trigger_load(file)
       
    def trigger_load(self, object_key):
        payload = {
            "loadType": "dataLoader",
            "entityType": self.entity,
            "relationshipType": "",
            "objectKey": object_key
        }
        logger.info(f"payload: {payload}")
        trigger_load_response = requests.post(self.load_url, headers=self.headers, data=json.dumps(payload))
        logger.info(f"response : {trigger_load_response}")
        job_id = trigger_load_response.json().get("id")
        logger.info(f"jobid : {job_id}")
        self.check_status(job_id)
       
    def check_status(self, job_id):
        load_url = f"{self.status_url}?jobId={job_id}&loadType=dataLoader"

        while True:
            status_response = requests.get(load_url, headers=self.headers)
            status = status_response.json().get('jobStatus')
            error_filename = status_response.json().get('errorFileLocation')
            logger.info(f"job_id:{job_id}|status: {status}|file:{self.file}")
            if status not in ['SCHEDULED', 'PROFILES_LOAD_IN_PROGRESS']:
                break
            time.sleep(30)  # wait for 10 seconds before polling again
        return self.process_failed_jobs(status, job_id, status_response, error_filename)

    def process_failed_jobs(self, status, job_id, status_response, error_filename):
        # logger.info(f"{job_id}|{self.file}|status_code{status_response.status_code}")
        if status=='COMPLETED_WITH_ERRORS':
            error_url = f"{self.error_url}?jobId={job_id}&loadType=dataLoader&errorFilename={error_filename}"
            logger.info(f"error_url: {error_url}")
            error_response = requests.get(error_url, headers=self.headers)
            logger.info(f"error_response.status_code: {error_response.status_code}")
            logger.error(f"error_response:{error_response.text}")

        elif status =='ERROR':
            logger.error(f"{job_id}|{self.file}|{status_response}")
        
        
# Initialize and run the DataLoader
sap_hcp_loader = DataLoader(
    client_id="a9bfd4b0970e4ab1a38c80d70adf94e1",
    client_secret="33aa6063a6d944fDA6BdbBdD431d4c25",
    s3_bucket="adl-base-customer-mdm-etl-dev-226aog",
    s3_prefix="enriched/FS_SAP_DC_CUST/API_TESTING/",
    entity="HCP"
)

sap_hcp_loader.detect_files()
