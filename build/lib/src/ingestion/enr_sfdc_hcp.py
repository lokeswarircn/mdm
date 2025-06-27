import os
import boto3
import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, struct, collect_list, lit, array, from_json, when, concat, regexp_replace
from pyspark.sql.types import StringType, ArrayType, StructType, MapType

# Initialize Spark session with S3 access
s3_client = boto3.client('s3')
spark = SparkSession.builder \
    .appName("ParquetToJSON") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain") \
    .getOrCreate()

# S3 path to your Parquet file
hcp_comb_trusted = "s3://adl-base-customer-md-q5s69ag5wucw6xpfyke6a84xdtyxause1b-s3alias/raw/SFDC_Extract_parquet/HCP/"
hcp_comb_enriched = "s3://adl-base-customer-md-zjgnb31idg7ozb8ttimo3zojpuffguse1b-s3alias/enriched/FS_SFDC_extract/HCP_extract/"
#hco_comb_trusted = "s3://adl-base-customer-md-9b69xa1sw6t9jtkd3azyi3rjois1huse1b-s3alias/trusted/FS_SFDC_ACC_CUST/HCO_CUST_ADDR_COMB/"
#hco_comb_enriched = "s3://adl-base-customer-md-zjgnb31idg7ozb8ttimo3zojpuffguse1b-s3alias/enriched/FS_SFDC_ACC_CUST/HCO_comb/"


# HCP Process
try:
    # Read the Parquet file from S3
    hcp_df = spark.read.parquet(hcp_comb_trusted)

    # Replace nulls with empty strings for all columns
    hcp_df = hcp_df.select([when(col(c).isNull(), "").otherwise(col(c)).alias(c) for c in hcp_df.columns])

    # Print the original schema
    print("Original Schema:")
    hcp_df.printSchema()

    # Transformation logic
    hcp_transformed_df = hcp_df.select(
        lit("configuration/entityTypes/HCP").alias("type"),
        struct(
            array(
                struct(
                    lit("configuration/entityTypes/HCP/attributes/Name").alias("type"),
                    col("Name").alias("value")
                )
            ).alias("Name"),
            array(
                struct(
                    lit("configuration/entityTypes/HCP/attributes/Name1").alias("type"),
                    col("Name1").alias("value")
                )
            ).alias("Name1"),
            array(
                struct(
                    lit("configuration/entityTypes/HCP/attributes/Name2").alias("type"),
                    col("Name2").alias("value")
                )
            ).alias("Name2"),
            array(
                struct(
                    lit("configuration/entityTypes/HCP/attributes/Name3").alias("type"),
                    col("Name3").alias("value")
                )
            ).alias("Name3"),
            array(
                struct(
                    struct(
                        array(
                            struct(
                                lit("configuration/entityTypes/HCP/attributes/CompanyCodeNested/attributes/CompanyCode").alias("type"),
                                col("CompanyCode").alias("value")
                            )
                        ).alias("CompanyCode"),
                        array(
                            struct(
                                lit("configuration/entityTypes/HCP/attributes/CompanyCodeNested/attributes/CompanyCodeDescription").alias("type"),
                                col("CompanyCodeDescription").alias("value")
                            )
                        ).alias("CompanyCodeDescription")
                    ).alias("value")
                )
            ).alias("CompanyCodeNested"),
            array(
                struct(
                    lit("configuration/entityTypes/HCP/attributes/VATregistrationnumber").alias("type"),
                    col("VATregistrationnumber").alias("value")
                )
            ).alias("VATregistrationnumber"),
            array(
                struct(
                    lit("configuration/entityTypes/HCP/attributes/StoreID").alias("type"),
                    col("StoreID").alias("value")
                )
            ).alias("StoreID"),
            array(
                struct(
                    lit("configuration/entityTypes/HCP/attributes/AccountGroup").alias("type"),
                    col("AccountGroup").alias("value")
                )
            ).alias("AccountGroup"),
            array(
                struct(
                    lit("configuration/entityTypes/HCP/attributes/AccountGroupDescription").alias("type"),
                    col("AccountGroupDescription").alias("value")
                )
            ).alias("AccountGroupDescription"),
            array(
                struct(
                    lit("configuration/entityTypes/HCP/attributes/CustomerClassification").alias("type"),
                    col("CustomerClassification").alias("value")
                )
            ).alias("CustomerClassification"),
            array(
                struct(
                    lit("configuration/entityTypes/HCP/attributes/CustomerClassificationDescription").alias("type"),
                    col("CustomerClassificationDescription").alias("value")
                )
            ).alias("CustomerClassificationDescription"),
            array(
                struct(
                    lit("configuration/entityTypes/HCP/attributes/IndustryCode").alias("type"),
                    col("IndustryCode").alias("value")
                )
            ).alias("IndustryCode"),
            array(
                struct(
                    lit("configuration/entityTypes/HCP/attributes/IndustryCodeDescription").alias("type"),
                    col("IndustryCodeDescription").alias("value")
                )
            ).alias("IndustryCodeDescription"),
            array(
                struct(
                    lit("configuration/entityTypes/HCP/attributes/Fax").alias("type"),
                    col("Fax_number").alias("value")
                )
            ).alias("Fax"),
            array(
                struct(
                    lit("configuration/entityTypes/HCP/attributes/Franchise").alias("type"),
                    col("Franchise").alias("value")
                )
            ).alias("Franchise"),
            array(
                struct(
                    lit("configuration/entityTypes/HCP/attributes/FranchiseDescription").alias("type"),
                    col("FranchiseDescription").alias("value")
                )
            ).alias("FranchiseDescription"),
            array(
                struct(
                    struct(
                        array(
                            struct(
                                lit("configuration/entityTypes/HCP/attributes/SalesOrg/attributes/SalesOrg").alias("type"),
                                col("SalesOrg_SalesOrg").alias("value")
                            )
                        ).alias("SalesOrg"),
                        array(
                            struct(
                                lit("configuration/entityTypes/HCP/attributes/SalesOrg/attributes/CurrencySalesOrg").alias("type"),
                                col("CurrencySalesOrg").alias("value")
                            )
                        ).alias("CurrencySalesOrg")
                    ).alias("value")
                )
            ).alias("SalesOrg"),
            array(
                struct(
                    lit("configuration/entityTypes/HCP/attributes/CreatedDate").alias("type"),
                    col("run_date").alias("value")
                )
            ).alias("CreatedDate"),
            array(
                struct(
                    lit("configuration/entityTypes/HCP/attributes/PreferredMethodofCommunication").alias("type"),
                    col("Name").alias("value")
                )
            ).alias("PreferredMethodofCommunication"),
            array(
                struct(
                    struct(
                        array(
                            struct(
                                lit("configuration/entityTypes/Location/attributes/AddressLine1").alias('type'),
                                col("AddressLine1").alias("value")
                            )
                        ).alias("AddressLine1"),
                        array(
                            struct(
                                lit("configuration/entityTypes/Location/attributes/City").alias('type'),
                                col("City").alias("value")
                            )
                        ).alias("City"),
                        array(
                            struct(
                                lit("configuration/entityTypes/Location/attributes/StateProvince").alias('type'),
                                col("Region").alias("value")
                            )
                        ).alias("StateProvince"),
                        array(
                            struct(
                                lit("configuration/entityTypes/Location/attributes/Country").alias('type'),
                                col("Country").alias("value")
                            )
                        ).alias("Country"),
                        array( # Added this array for PostalCode to align with the pattern
                            struct(
                                struct(
                                    array(
                                        struct(
                                            lit("configuration/entityTypes/Location/attributes/PostalCode/attributes/PostalCode").alias("type"),
                                            col("PostalCode").alias("value")
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
                                    lit("configuration/sources/FS_SFDC_ACC_CUST").alias("type"),
                                    lit("surrogate").alias("value")
                                )
                            ).alias("crosswalks")
                    ).alias("refEntity"),
                        struct(
                            lit("configuration/relationTypes/HCPHasAddress").alias("type"),
                            array(
                                struct(
                                    lit("configuration/sources/FS_SFDC_ACC_CUST").alias("type"),
                                    concat(lit("FS_SFDC_ACC_CUST - "), col("Identifier_ID").cast(StringType()), lit(" -Primary")).alias("value")
                                )
                            ).alias("crosswalks")
                    ).alias("refRelation")
                )
            ).alias("Address") # This 'Address' was previously outside the outer struct, causing issues.
 # This 'refRelation' was previously duplicated in terms of aliasing

        ).alias("attributes"),
                    array( # This 'crosswalks' was previously outside the outer struct, causing issues.
                struct(
                    lit("configuration/sources/FS_SFDC_ACC_CUST").alias("type"),
                    col("Identifier_ID").alias("value")
                )
            ).alias("crosswalks")
    )
    # hcp_transformed_df = hcp_transformed_df.select(collect_list(struct("*")).alias("data"))


    # Print DF type
    print(type(hcp_transformed_df))

    # Collect and print one row to check the structure
    print(hcp_transformed_df.take(1))

    # Print the schema after transformations
    print("\nSchema after transformations:")
    hcp_transformed_df.printSchema()
    hcp_transformed_df=hcp_transformed_df.limit(10)
    hcp_transformed_df.repartition(5).write.mode("overwrite").json(hcp_comb_enriched)
    print(f"*"*15, "HCP Json Enriched Load Complete", "*"*15)
    print(f"*"*15, "HCP Cosmetic Changes Start", "*"*15)
    s3_bucket = "adl-base-customer-mdm-etl-dev-226aog"
    prefix = "enriched/FS_SFDC_ACC_CUST/HCP_comb"  # S3 folder path

# List all objects in the folder
    json_files = s3_client.list_objects_v2(Bucket=s3_bucket, Prefix=prefix)
    if 'Contents' in json_files:
        json_files = [obj['Key'] for obj in json_files['Contents'] if obj['Key'].endswith('.json')]


    print(f"json files: {json_files}")

    for s3_key in json_files:
        response = s3_client.get_object(Bucket=s3_bucket, Key=s3_key)
        original_content = response['Body'].read().decode('utf-8')
        modified_content = original_content.strip().replace('\n', ',')
        print(f"length of modified_content: {len(modified_content)}")
        modified_content2 = f"[{','.join(modified_content)}]"
        modified_content3 = f"[{''.join(modified_content)}]"
        print(f"original_content: {original_content}")
        print(f"modified_content: {modified_content}")
        print(f"modified_content2: {modified_content2}")
        print(f"modified_content3: {modified_content3}")

        s3_client.put_object(
            Bucket=s3_bucket,
            Key=s3_key,
            Body=modified_content3.encode('utf-8'), # Encode back to bytes
            ContentType='application/json' # Assuming it's still intended to be JSON
        )

    # for json_file in json_files:
    #     json_content = []
    #     with open(json_file, 'r') as file:
    #         content = file.read().strip().replace('\n', ',')
    #         json_content.append(content)

    #     # Enclose the entire JSON content with an array
    #     final_json = f"[{','.join(json_content)}]"
    #     print(f"final_json: {final_json}")
    #     print(f"*"*15, "-", "*"*15)


    #     with open(json_file, 'w') as file:
    #         file.write(final_json)


except Exception as e:
    print(f"HCP Exception: {e}")
    raise(e)

finally:
    print("Finally Block")
    # spark.stop() # Uncomment this line if you want to stop the Spark session after the HCP process




# # HCO Process
# try:
#     # Read the Parquet file from S3
#     hco_df = spark.read.parquet(hco_comb_trusted)

#     # Replace nulls with empty strings for all columns
#     hco_df = hco_df.select([when(col(c).isNull(), "").otherwise(col(c)).alias(c) for c in hco_df.columns])

#     # Print the original schema
#     print("Original Schema:")
#     hco_df.printSchema()

#     # Transformation logic
#     hco_transformed_df = hco_df.select(
#         lit("configuration/entityTypes/HCO").alias("type"),
#         struct(
#             array(
#                 struct(
#                     lit("configuration/entityTypes/HCO/attributes/Name").alias("type"),
#                     col("Name").alias("value")
#                 )
#             ).alias("Name"),
#             array(
#                 struct(
#                     lit("configuration/entityTypes/HCO/attributes/Name1").alias("type"),
#                     col("Name1").alias("value")
#                 )
#             ).alias("Name1"),
#             array(
#                 struct(
#                     lit("configuration/entityTypes/HCO/attributes/Name2").alias("type"),
#                     col("Name2").alias("value")
#                 )
#             ).alias("Name2"),
#             array(
#                 struct(
#                     lit("configuration/entityTypes/HCO/attributes/Name3").alias("type"),
#                     col("Name3").alias("value")
#                 )
#             ).alias("Name3"),
#             array(
#                 struct(
#                     struct(
#                         array(
#                             struct(
#                                 lit("configuration/entityTypes/HCO/attributes/CompanyCodeNested/attributes/CompanyCode").alias("type"),
#                                 col("CompanyCode").alias("value")
#                             )
#                         ).alias("CompanyCode"),
#                         array(
#                             struct(
#                                 lit("configuration/entityTypes/HCO/attributes/CompanyCodeNested/attributes/CompanyCodeDescription").alias("type"),
#                                 col("CompanyCodeDescription").alias("value")
#                             )
#                         ).alias("CompanyCodeDescription")
#                     ).alias("value")
#                 )
#             ).alias("CompanyCodeNested"),
#             array(
#                 struct(
#                     lit("configuration/entityTypes/HCO/attributes/VATregistrationnumber").alias("type"),
#                     col("VATregistrationnumber").alias("value")
#                 )
#             ).alias("VATregistrationnumber"),
#             array(
#                 struct(
#                     lit("configuration/entityTypes/HCO/attributes/StoreID").alias("type"),
#                     col("StoreID").alias("value")
#                 )
#             ).alias("StoreID"),
#             array(
#                 struct(
#                     lit("configuration/entityTypes/HCO/attributes/AccountGroup").alias("type"),
#                     col("AccountGroup").alias("value")
#                 )
#             ).alias("AccountGroup"),
#             array(
#                 struct(
#                     lit("configuration/entityTypes/HCO/attributes/AccountGroupDescription").alias("type"),
#                     col("AccountGroupDescription").alias("value")
#                 )
#             ).alias("AccountGroupDescription"),
#             array(
#                 struct(
#                     lit("configuration/entityTypes/HCO/attributes/CustomerClassification").alias("type"),
#                     col("CustomerClassification").alias("value")
#                 )
#             ).alias("CustomerClassification"),
#             array(
#                 struct(
#                     lit("configuration/entityTypes/HCO/attributes/CustomerClassificationDescription").alias("type"),
#                     col("CustomerClassificationDescription").alias("value")
#                 )
#             ).alias("CustomerClassificationDescription"),
#             array(
#                 struct(
#                     lit("configuration/entityTypes/HCO/attributes/IndustryCode").alias("type"),
#                     col("IndustryCode").alias("value")
#                 )
#             ).alias("IndustryCode"),
#             array(
#                 struct(
#                     lit("configuration/entityTypes/HCO/attributes/IndustryCodeDescription").alias("type"),
#                     col("IndustryCodeDescription").alias("value")
#                 )
#             ).alias("IndustryCodeDescription"),
#             array(
#                 struct(
#                     lit("configuration/entityTypes/HCO/attributes/Fax").alias("type"),
#                     col("Fax_number").alias("value")
#                 )
#             ).alias("Fax"),
#             array(
#                 struct(
#                     lit("configuration/entityTypes/HCO/attributes/Franchise").alias("type"),
#                     col("Franchise").alias("value")
#                 )
#             ).alias("Franchise"),
#             array(
#                 struct(
#                     lit("configuration/entityTypes/HCO/attributes/FranchiseDescription").alias("type"),
#                     col("FranchiseDescription").alias("value")
#                 )
#             ).alias("FranchiseDescription"),
#             array(
#                 struct(
#                     struct(
#                         array(
#                             struct(
#                                 lit("configuration/entityTypes/HCO/attributes/SalesOrg/attributes/SalesOrg").alias("type"),
#                                 col("SalesOrg_SalesOrg").alias("value")
#                             )
#                         ).alias("SalesOrg"),
#                         array(
#                             struct(
#                                 lit("configuration/entityTypes/HCO/attributes/SalesOrg/attributes/CurrencySalesOrg").alias("type"),
#                                 col("CurrencySalesOrg").alias("value")
#                             )
#                         ).alias("CurrencySalesOrg")
#                     ).alias("value")
#                 )
#             ).alias("SalesOrg"),
#             array(
#                 struct(
#                     lit("configuration/entityTypes/HCO/attributes/CreatedDate").alias("type"),
#                     col("run_date").alias("value")
#                 )
#             ).alias("CreatedDate"),
#             array(
#                 struct(
#                     lit("configuration/entityTypes/HCO/attributes/PreferredMethodofCommunication").alias("type"),
#                     col("Name").alias("value")
#                 )
#             ).alias("PreferredMethodofCommunication"),
#             array(
#                 struct(
#                     struct(
#                         array(
#                             struct(
#                                 lit("configuration/entityTypes/Location/attributes/AddressLine1").alias('type'),
#                                 col("AddressLine1").alias("value")
#                             )
#                         ).alias("AddressLine1"),
#                         array(
#                             struct(
#                                 lit("configuration/entityTypes/Location/attributes/City").alias('type'),
#                                 col("City").alias("value")
#                             )
#                         ).alias("City"),
#                         array(
#                             struct(
#                                 lit("configuration/entityTypes/Location/attributes/StateProvince").alias('type'),
#                                 col("Region").alias("value")
#                             )
#                         ).alias("StateProvince"),
#                         array(
#                             struct(
#                                 lit("configuration/entityTypes/Location/attributes/Country").alias('type'),
#                                 col("Country").alias("value")
#                             )
#                         ).alias("Country"),
#                         array( # Added this array for PostalCode to align with the pattern
#                             struct(
#                                 struct(
#                                     array(
#                                         struct(
#                                             lit("configuration/entityTypes/Location/attributes/PostalCode/attributes/PostalCode").alias("type"),
#                                             col("PostalCode").alias("value")
#                                         )
#                                     ).alias("PostalCode")
#                                 ).alias("value")
#                             )
#                         ).alias("PostalCode")
#                     ).alias("value"),
#                         struct(
#                             lit("configuration/entityTypes/Location").alias("type"),
#                             array(
#                                 struct(
#                                     lit("configuration/sources/FS_SFDC_ACC_CUST").alias("type"),
#                                     lit("surrogate").alias("value")
#                                 )
#                             ).alias("crosswalks")
#                     ).alias("refEntity"),
#                         struct(
#                             lit("configuration/relationTypes/HCOHasAddress").alias("type"),
#                             array(
#                                 struct(
#                                     lit("configuration/sources/FS_SFDC_ACC_CUST").alias("type"),
#                                     concat(lit("FS_SFDC_ACC_CUST - "), col("Identifier_ID").cast(StringType()), lit(" -Primary")).alias("value")
#                                 )
#                             ).alias("crosswalks")
#                     ).alias("refRelation")
#                 )
#             ).alias("Address") # This 'Address' was previously outside the outer struct, causing issues.
#  # This 'refRelation' was previously duplicated in terms of aliasing

#         ).alias("attributes"),
#                     array( # This 'crosswalks' was previously outside the outer struct, causing issues.
#                 struct(
#                     lit("configuration/sources/FS_SFDC_ACC_CUST").alias("type"),
#                     col("Identifier_ID").alias("value")
#                 )
#             ).alias("crosswalks")
#     )
#     # hco_transformed_df = hco_transformed_df.select(collect_list(struct("*")).alias("data"))


#     # Print DF type
#     print(type(hco_transformed_df))

#     # Collect and print one row to check the structure
#     print(hco_transformed_df.take(1))

#     # Print the schema after transformations
#     print("\nSchema after transformations:")
#     hco_transformed_df.printSchema()
#     hco_transformed_df=hco_transformed_df.limit(10)
#     hco_transformed_df.repartition(5).write.mode("overwrite").json(hco_comb_enriched)
#     print(f"*"*15, "HCO Json Enriched Load Complete", "*"*15)
#     print(f"*"*15, "HCO Cosmetic Changes Start", "*"*15)
#     s3_bucket = "adl-base-customer-mdm-etl-dev-226aog"
#     prefix = "enriched/FS_SFDC_ACC_CUST/HCO_comb"  # S3 folder path

# # List all objects in the folder
#     json_files = s3_client.list_objects_v2(Bucket=s3_bucket, Prefix=prefix)
#     if 'Contents' in json_files:
#         json_files = [obj['Key'] for obj in json_files['Contents'] if obj['Key'].endswith('.json')]


#     print(f"json files: {json_files}")

#     for s3_key in json_files:
#         response = s3_client.get_object(Bucket=s3_bucket, Key=s3_key)
#         original_content = response['Body'].read().decode('utf-8')
#         modified_content = original_content.strip().replace('\n', ',')
#         print(f"length of modified_content: {len(modified_content)}")
#         modified_content2 = f"[{','.join(modified_content)}]"
#         modified_content3 = f"[{''.join(modified_content)}]"
#         print(f"original_content: {original_content}")
#         print(f"modified_content: {modified_content}")
#         print(f"modified_content2: {modified_content2}")
#         print(f"modified_content3: {modified_content3}")

#         s3_client.put_object(
#             Bucket=s3_bucket,
#             Key=s3_key,
#             Body=modified_content3.encode('utf-8'), # Encode back to bytes
#             ContentType='application/json' # Assuming it's still intended to be JSON
#         )

#     # for json_file in json_files:
#     #     json_content = []
#     #     with open(json_file, 'r') as file:
#     #         content = file.read().strip().replace('\n', ',')
#     #         json_content.append(content)

#     #     # Enclose the entire JSON content with an array
#     #     final_json = f"[{','.join(json_content)}]"
#     #     print(f"final_json: {final_json}")
#     #     print(f"*"*15, "-", "*"*15)


#     #     with open(json_file, 'w') as file:
#     #         file.write(final_json)


# except Exception as e:
#     print(f"HCO Exception: {e}")
#     raise(e)

# finally:
#     print("Finally Block")
#     # spark.stop() # Uncomment this line if you want to stop the Spark session after the HCO process
