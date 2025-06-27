"""input args for each layer"""


class RawArgs(object):
    """Raw layer args"""
    def __init__(self,
                 args):
        self.source_s3_bucket = args.get('SOURCE_PATH').split('/')[0]
        self.source_folder_key = '/'.join(args.get('SOURCE_PATH').split('/')[1:])
        self.target_s3_bucket = args.get('TARGET_PATH').split('/')[0]
        self.target_folder_key = '/'.join(args.get('TARGET_PATH').split('/')[1:])
        self.config_s3_bucket = args.get('CONFIG_PATH').split('/')[0]
        self.config_folder_key = '/'.join(args.get('CONFIG_PATH').split('/')[1:])
        self.dynamodb_table = args.get('DYNAMODB_TABLE')
        self.table_str = args.get('TABLE_LIST', '')
        self.exclude_tables_str = args.get('EXCLUDE_TABLES', "")
        self.success_file = args.get('SUCCESS_FILE', "")


class TrustedArgs(object):
    """Trusted layer args"""
    def __init__(self,
                 args):
        self.source_s3_bucket = args.get('SOURCE_PATH').split('/')[0]
        self.source_folder_key = '/'.join(args.get('SOURCE_PATH').split('/')[1:])
        self.target_s3_bucket = args.get('TARGET_PATH').split('/')[0]
        self.target_folder_key = '/'.join(args.get('TARGET_PATH').split('/')[1:])
        self.config_s3_bucket = args.get('CONFIG_PATH').split('/')[0]
        self.config_folder_key = '/'.join(args.get('CONFIG_PATH').split('/')[1:])
        self.target_database = args.get('TARGET_DATABASE')
        self.dynamodb_table = args.get('DYNAMODB_TABLE')
        self.table_str = args.get('TABLE_LIST', '')
        self.exclude_tables_str = args.get('EXCLUDE_TABLES', "")
        self.drop_tables = args.get('DROP_TABLES', "")


class EnrichedArgs(object):
    """Enriched layer args"""
    def __init__(self,
                 args):
        self.source_s3_bucket = args.get('SOURCE_PATH').split('/')[0]
        self.source_folder_key = '/'.join(args.get('SOURCE_PATH').split('/')[1:])
        self.target_s3_bucket = args.get('TARGET_PATH').split('/')[0]
        self.target_folder_key = '/'.join(args.get('TARGET_PATH').split('/')[1:])
        self.config_s3_bucket = args.get('CONFIG_PATH').split('/')[0]
        self.config_folder_key = '/'.join(args.get('CONFIG_PATH').split('/')[1:])
        self.target_database = args.get('TARGET_DATABASE')
        self.dynamodb_table = args.get('DYNAMODB_TABLE')
        self.table_str = args.get('TABLE_LIST', '')
        self.exclude_tables_str = args.get('EXCLUDE_TABLES', "")
        self.drop_tables = args.get('DROP_TABLES', "")


class BusinessArgs(object):
    """Business args"""
    def __init__(self,
                 args):
        self.target_s3_bucket = args.get('TARGET_PATH').split('/')[0]
        self.target_folder_key = '/'.join(args.get('TARGET_PATH').split('/')[1:])
        self.target_database = args.get('TARGET_DATABASE')
        self.enriched_database = args.get('ENRICHED_DATABASE')
        self.dynamodb_table = args.get('DYNAMODB_TABLE')
        self.business_table_str = args.get('BUSINESS_TABLES', '')
        self.table_str = args.get('TABLE_LIST', "")
        self.drop_tables = args.get('DROP_TABLES', "")
