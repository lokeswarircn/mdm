def parse_s3_uri(s3_uri: str) -> tuple:

    parts = s3_uri.replace('s3://', '').split('/', 1)
    return parts[0], parts[1] if len(parts) > 1 else ''

# Example usage
s3_uri = "s3://${var.artifact_bucket}/adl-customer-mdm-etl/releases/${var.artifacts_version}/adl_Customer_MDM_ETL-1.0.0-py3.6.egg"

bucket, prefix = parse_s3_uri(s3_uri)
print(f"Bucket: {bucket}")
print(f"Prefix: {prefix}")