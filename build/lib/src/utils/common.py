"""common utils"""

from datetime import datetime

current_date = datetime.today()


def get_folder_key(table_name, target_key=None):
    """return folder path"""
    if target_key:
        return target_key + "/" + table_name
    return table_name


def get_current_timestamp():
    """return current timestamp"""
    return current_date.strftime('%Y-%m-%d') + "T" + str(current_date.strftime('%H-%M-%S'))


def get_processed_tables(object_properties, args):
    """check for all the exculde and include table and return a list of tables to process"""

    tables_to_be_processed = object_properties["objects"].keys()

    """overwrite table list provided by user input and check if table exist in config"""
    if args.table_str:
        tables_to_be_processed = [table_name for table_name in args.table_str.split(',')
                                  if table_name in object_properties["objects"]]

    """exclude tables which are not to be ingested as part of this run"""
    if args.exclude_tables_str:
        tables_to_be_processed = [table_name for table_name in tables_to_be_processed
                                  if table_name not in args.exclude_tables_str.split(',')]

    return tables_to_be_processed


def get_s3_data_location(bucket, folder, target_key=None):
    """format s3"""
    file_system_prefix = "s3://"
    if target_key:
        return file_system_prefix + bucket + "/" + target_key + "/" + folder
    return file_system_prefix + bucket + "/" + folder


class TableFailedException(Exception):
    """table failed exception"""
    pass
