import sys
from src.utils.params import Params
class Environment:
    @classmethod
    def get_env(cls) -> str:
        return Params(sys.argv).get('env')
class s3:
    @classmethod
    def parse_s3_uri(cls,s3_uri: str) -> tuple:
        parts = s3_uri.replace('s3://', '').split('/', 1)
        return parts[0], parts[1] if len(parts) > 1 else ''