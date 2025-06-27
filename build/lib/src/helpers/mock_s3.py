from typing import Dict, List, Any
import json
import os

class MockS3Client:
    def __init__(self, base_path: str = "/tmp/mock_s3"):
        self.base_path = base_path
        os.makedirs(base_path, exist_ok=True)
    
    def put_object(self, Bucket: str, Key: str, Body: bytes, ContentType: str = None) -> Dict[str, Any]:
        """Mock S3 put_object"""
        file_path = os.path.join(self.base_path, Bucket, Key)
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        
        with open(file_path, 'wb') as f:
            f.write(Body)
        return {'ResponseMetadata': {'HTTPStatusCode': 200}}

    def get_object(self, Bucket: str, Key: str) -> Dict[str, Any]:
        """Mock S3 get_object"""
        file_path = os.path.join(self.base_path, Bucket, Key)
        try:
            with open(file_path, 'rb') as f:
                content = f.read()
            return {'Body': MockBody(content)}
        except FileNotFoundError:
            raise Exception(f"File not found: {file_path}")

    def list_objects_v2(self, Bucket: str, Prefix: str = "") -> Dict[str, Any]:
        """Mock S3 list_objects_v2"""
        bucket_path = os.path.join(self.base_path, Bucket)
        prefix_path = os.path.join(bucket_path, Prefix)
        
        if not os.path.exists(prefix_path):
            return {}
            
        contents = []
        for root, _, files in os.walk(prefix_path):
            for file in files:
                full_path = os.path.join(root, file)
                key = os.path.relpath(full_path, bucket_path)
                contents.append({'Key': key})
        
        return {'Contents': contents} if contents else {}

class MockBody:
    def __init__(self, content: bytes):
        self.content = content
    
    def read(self) -> bytes:
        return self.content