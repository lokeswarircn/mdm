"""aws utils"""
import boto3
import json
from boto3.dynamodb import conditions


def read_json(bucket, key):
    """read json from s3"""

    s3 = boto3.resource('s3')
    obj = s3.Object(bucket, key)
    config = json.load(obj.get()['Body'])
    return config


def list_objects_s3(bucket=None, prefix=None):
    """list all the files in s3"""
    client = boto3.client('s3')
    paginator = client.get_paginator('list_objects')
    operation_parameters = {'Bucket': bucket}

    if prefix:
        if not prefix.endswith('/'):
            prefix = prefix + "/"
        operation_parameters['Prefix'] = prefix
        operation_parameters['Delimiter'] = "/"

    page_iterator = paginator.paginate(**operation_parameters)

    files_keys = []
    for page in page_iterator:
        files_keys.extend([i['Key'] for i in page.get('Contents', [])])

    return files_keys


def move_objects_s3(source_bucket=None, target_bucket=None, target_folder_key=None, file_keys=None):
    """
    This function moves list of files from S3 folder and another folder
    :return: None
    """
    s3_resource = boto3.resource('s3')
    for file_key in file_keys:
        copy_source = {
            'Bucket': source_bucket,
            'Key': file_key
        }
        dest_file = file_key.split("/")[-1]
        dest_key = target_folder_key + "/" + dest_file
        s3_resource.meta.client.copy(copy_source, target_bucket, dest_key)
        s3_resource.Object(source_bucket, file_key).delete()


def update_dynamodb_placeholder(dynamodb_table_name,
                                layer_name,
                                table_name,
                                source_prefix,
                                current_timestamp,
                                to_be_processed,
                                is_full_load=False):
    """update placeholder in dynamodb table"""

    dynamo_db_client = boto3.resource('dynamodb')

    update_str = "set to_be_processed = :e, source_prefix = :s"
    expression_dict = {":e": to_be_processed, ":s": source_prefix}

    if current_timestamp:
        update_str = update_str + ", ingestion_timestamp = :r"
        expression_dict[":r"] = current_timestamp

    if is_full_load:
        update_str = update_str + ", last_full_load_on = :f"
        expression_dict[":f"] = current_timestamp

    table = dynamo_db_client.Table(dynamodb_table_name)
    table.update_item(Key={
        'object_name': layer_name + "~" + table_name
    }, UpdateExpression=update_str,
        ExpressionAttributeValues=expression_dict,
        ReturnValues="UPDATED_NEW"
    )


def get_dynamo_db_placeholder(dynamodb_table_name, layer_name, table_name):
    """get the placeholder from dynamodb table"""

    dynamo_db_client = boto3.resource('dynamodb', region_name='us-east-1')
    table = dynamo_db_client.Table(dynamodb_table_name)
    key = layer_name + '~' + table_name
    response = table.query(
            KeyConditionExpression=conditions.Key('object_name').eq(key),
            FilterExpression=conditions.Key('to_be_processed').eq(True)
        ).get('Items', [])
    item = None
    if len(response) > 0:
        item = response[0].get('source_prefix', None)
    return item


def delete_success_file(bucket, success_folder, success_file):
    """delete the success file from bucket"""
    success_key = f"{success_folder}/{success_file}"
    _S3_RESOURCE = boto3.resource('s3')
    try:
        _S3_RESOURCE.meta.client.delete_object(
            Bucket=bucket,
            Key=success_key
        )
    except Exception as exp:
        return False
