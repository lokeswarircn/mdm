
import boto3
import sys
from src.helpers.data import Data
from src.helpers.logger import Logger
from src.helpers.args import Enriched
from pyspark.sql.functions import col, struct, collect_list, lit, array, from_json, when, concat, regexp_replace
from pyspark.sql.types import StringType, ArrayType, StructType, MapType
log = Logger.get_logger(__name__)
import os
from src.helpers.mock_s3 import MockS3Client

# Initialize S3 client based on environment
if os.getenv('ENV') == None:
    s3_client = MockS3Client()
else:
    import boto3
    s3_client = boto3.client('s3')

# ...existing code...
# S3 path to your Parquet file
# hco_comb_trusted = "s3://adl-base-customer-md-9b69xa1sw6t9jtkd3azyi3rjois1huse1b-s3alias/trusted/FS_DNB/DNB_CUST_ADDR_COMB/"
# hco_comb_enriched = "s3://adl-base-customer-md-zjgnb31idg7ozb8ttimo3zojpuffguse1b-s3alias/enriched/FS_DNB/"


# HCO Process
try:
    dnb_tst_path, dnb_enr_path, base_bucket, dnb_enr_prefix = Enriched(sys.argv).get_dnb_enr_args()
    log.info("Read data from Trusted Layer")
    hco_df = Data.read('file', dnb_tst_path)

    log.info("Original Schema:")
    hco_df.printSchema()

    # Transformation logic
    hco_transformed_df = hco_df.select(
        lit("configuration/entityTypes/HCO").alias("type"),
        struct(
            array(
                struct(
                    lit("configuration/entityTypes/HCO/attributes/Name").alias("type"),
                    col("Name").alias("value")
                )
            ).alias("Name"),
            array(
                struct(
                    lit("configuration/entityTypes/HCO/attributes/Name1").alias("type"),
                    col("Name1").alias("value")
                )
            ).alias("Name1"),
            array(
                struct(
                    lit("configuration/entityTypes/HCO/attributes/Name2").alias("type"),
                    col("Name2").alias("value")
                )
            ).alias("Name2"),
            array(
                struct(
                    lit("configuration/entityTypes/HCO/attributes/Name3").alias("type"),
                    col("Name3").alias("value")
                )
            ).alias("Name3"),
            #adding identifier and identifier type
            array(
            struct(
                struct(
                    array(
                        struct(
                            lit("configuration/entityTypes/HCO/attributes/Identifiers/attributes/Type").alias("type"),
                            col("Identifier_ID").alias("value")
                        ).alias("Identifiers")
                    ).alias("Identifiers"),
                    array(
                        struct(
                            lit("configuration/entityTypes/HCO/attributes/Identifiers/attributes/ID").alias("type"),
                            col("Identifier_type").alias("value")
                        ).alias("Identifier_type")
                    ).alias("Identifier_type")
                ).alias("value")
            )#.alias("Identifier_type")
        ).alias("Identifiers"),  
            #identifier type complete
            array(
                struct(
                    struct(
                        array(
                            struct(
                                lit("configuration/entityTypes/HCO/attributes/CompanyCodeNested/attributes/CompanyCode").alias("type"),
                                col("CompanyCode").alias("value")
                            )
                        ).alias("CompanyCode"),
                        array(
                            struct(
                                lit("configuration/entityTypes/HCO/attributes/CompanyCodeNested/attributes/CompanyCodeDescription").alias("type"),
                                col("CompanyCodeDescription").alias("value")
                            )
                        ).alias("CompanyCodeDescription")
                    ).alias("value")
                )
            ).alias("CompanyCodeNested"),
            array(
                struct(
                    lit("configuration/entityTypes/HCO/attributes/VATregistrationnumber").alias("type"),
                    col("VATregistrationnumber").alias("value")
                )
            ).alias("VATregistrationnumber"),
            array(
                struct(
                    lit("configuration/entityTypes/HCO/attributes/StoreID").alias("type"),
                    col("StoreID").alias("value")
                )
            ).alias("StoreID"),
            array(
                struct(
                    lit("configuration/entityTypes/HCO/attributes/AccountGroup").alias("type"),
                    col("AccountGroup").alias("value")
                )
            ).alias("AccountGroup"),
            array(
                struct(
                    lit("configuration/entityTypes/HCO/attributes/AccountGroupDescription").alias("type"),
                    col("AccountGroupDescription").alias("value")
                )
            ).alias("AccountGroupDescription"),
            array(
                struct(
                    lit("configuration/entityTypes/HCO/attributes/CustomerClassification").alias("type"),
                    col("CustomerClassification").alias("value")
                )
            ).alias("CustomerClassification"),
            array(
                struct(
                    lit("configuration/entityTypes/HCO/attributes/CustomerClassificationDescription").alias("type"),
                    col("CustomerClassificationDescription").alias("value")
                )
            ).alias("CustomerClassificationDescription"),
            array(
                struct(
                    lit("configuration/entityTypes/HCO/attributes/IndustryCode").alias("type"),
                    col("IndustryCode").alias("value")
                )
            ).alias("IndustryCode"),
            array(
                struct(
                    lit("configuration/entityTypes/HCO/attributes/IndustryCodeDescription").alias("type"),
                    col("IndustryCodeDescription").alias("value")
                )
            ).alias("IndustryCodeDescription"),
            array(
                struct(
                    lit("configuration/entityTypes/HCO/attributes/Fax").alias("type"),
                    col("Fax_number").alias("value")
                )
            ).alias("Fax"),
            array(
                struct(
                    lit("configuration/entityTypes/HCO/attributes/Franchise").alias("type"),
                    col("Franchise").alias("value")
                )
            ).alias("Franchise"),
            array(
                struct(
                    lit("configuration/entityTypes/HCO/attributes/FranchiseDescription").alias("type"),
                    col("FranchiseDescription").alias("value")
                )
            ).alias("FranchiseDescription"),
            array(
                struct(
                    struct(
                        array(
                            struct(
                                lit("configuration/entityTypes/HCO/attributes/SalesOrg/attributes/SalesOrg").alias("type"),
                                col("SalesOrg_SalesOrg").alias("value")
                            )
                        ).alias("SalesOrg"),
                        array(
                            struct(
                                lit("configuration/entityTypes/HCO/attributes/SalesOrg/attributes/CurrencySalesOrg").alias("type"),
                                col("CurrencySalesOrg").alias("value")
                            )
                        ).alias("CurrencySalesOrg")
                    ).alias("value")
                )
            ).alias("SalesOrg"),
            array(
                struct(
                    lit("configuration/entityTypes/HCO/attributes/CreatedDate").alias("type"),
                    col("run_date").alias("value")
                )
            ).alias("CreatedDate"),
            array(
                struct(
                    lit("configuration/entityTypes/HCO/attributes/PreferredMethodofCommunication").alias("type"),
                    col("Name").alias("value")
                )
            ).alias("PreferredMethodofCommunication"),
            array(
                struct(
                    struct(
                        array(
                            struct(
                                lit("configuration/entityTypes/Location/attributes/AddressLine1").alias('type'),
                                col("Address_value_AddressLine1").alias("value")
                            )
                        ).alias("AddressLine1"),
                        array(
                            struct(
                                lit("configuration/entityTypes/Location/attributes/City").alias('type'),
                                col("Address_value_City").alias("value")
                            )
                        ).alias("City"),
                        array(
                            struct(
                                lit("configuration/entityTypes/Location/attributes/StateProvince").alias('type'),
                                col("Address_value_Region").alias("value")
                            )
                        ).alias("StateProvince"),
                        array(
                            struct(
                                lit("configuration/entityTypes/Location/attributes/Country").alias('type'),
                                col("Address_value_Country").alias("value")
                            )
                        ).alias("Country"),
                        array( # Added this array for PostalCode to align with the pattern
                            struct(
                                struct(
                                    array(
                                        struct(
                                            lit("configuration/entityTypes/Location/attributes/PostalCode/attributes/PostalCode").alias("type"),
                                            col("Address_value_Zip").alias("value")
                                        )
                                    ).alias("PostalCode")
                                ).alias("value")
                            )
                        ).alias("PostalCode")
                    ).alias("value"),
                        struct(
                            lit("configuration/entityTypes/Location").alias("type"),
                            array(
                                struct(
                                    lit("configuration/sources/DNB").alias("type"),
                                    lit("surrogate").alias("value")
                                )
                            ).alias("crosswalks")
                    ).alias("refEntity"),
                        struct(
                            lit("configuration/relationTypes/HCOHasAddress").alias("type"),
                            array(
                                struct(
                                    lit("configuration/sources/DNB").alias("type"),
                                    concat(lit("DNB - "), col("Identifier_ID").cast(StringType()), lit(" -Primary")).alias("value")
                                )
                            ).alias("crosswalks")
                    ).alias("refRelation")
                )
            ).alias("Address") # This 'Address' was previously outside the outer struct, causing issues.
 # This 'refRelation' was previously duplicated in terms of aliasing

        ).alias("attributes"),
                    array( # This 'crosswalks' was previously outside the outer struct, causing issues.
                struct(
                    lit("configuration/sources/DNB").alias("type"),
                    col("Identifier_ID").alias("value")
                )
            ).alias("crosswalks")
    )
    # hco_transformed_df = hco_transformed_df.select(collect_list(struct("*")).alias("data"))


    # Print DF type
    log.info(type(hco_transformed_df))

    # Collect and print one row to check the structure
    log.info(hco_transformed_df.take(1))

    # Print the schema after transformations
    log.info("\nSchema after transformations:")
    hco_transformed_df.printSchema()
    hco_transformed_df=hco_transformed_df.limit(10)
    # hco_transformed_df.repartition(5).write.mode("overwrite").json(hco_comb_enriched)
    Data.write('json', hco_transformed_df, dnb_enr_path)

# List all objects in the folder
    json_files = s3_client.list_objects_v2(Bucket=base_bucket, Prefix=dnb_enr_prefix)
    if 'Contents' in json_files:
        json_files = [obj['Key'] for obj in json_files['Contents'] if obj['Key'].endswith('.json')]


    log.info(f"json files: {json_files}")

    for s3_key in json_files:
        response = s3_client.get_object(Bucket=base_bucket, Key=s3_key)
        original_content = response['Body'].read().decode('utf-8')
        modified_content = original_content.strip().replace('\n', ',')
        log.info(f"length of modified_content: {len(modified_content)}")
        modified_content2 = f"[{','.join(modified_content)}]"
        modified_content3 = f"[{''.join(modified_content)}]"
        log.info(f"original_content: {original_content}")
        log.info(f"modified_content: {modified_content}")
        log.info(f"modified_content2: {modified_content2}")
        log.info(f"modified_content3: {modified_content3}")

        s3_client.put_object(
            Bucket=base_bucket,
            Key=s3_key,
            Body=modified_content3.encode('utf-8'), # Encode back to bytes
            ContentType='application/json' # Assuming it's still intended to be JSON
        )

except Exception as error:
    log.error(f"Unable to buil/write enriched: {str(error)}")
    raise Exception(error)

finally:
    log.info("Execution Complete!")