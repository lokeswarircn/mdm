from pyspark.sql import SparkSession
from pyspark.sql import Row

# Initialize SparkSession
spark = SparkSession.builder.appName("SparkDataframe").getOrCreate()
spark.conf.set("spark.sql.legacy.parquet.int96RebaseModeInWrite", "LEGACY")
# Execute SQL query and create a DataFrame
df = spark.sql(""" select md5(CONCAT(
							coalesce(Name, 'Name'),
							coalesce(AlternativeName, 'AlternativeName'),
							coalesce(VATregistrationnumber, 'VATregistrationnumber'),
							coalesce(StatusInformation_Status, 'StatusInformation_Status'),
							coalesce(Phone_number, 'Phone_number'),
							coalesce(DunsNumber, 'DunsNumber'),
							coalesce(WebsiteURL,'WebsiteURL'),
					        coalesce(Standalone, 'Standalone'),
					        coalesce(IsFortune1000Listed, 'IsFortune1000Listed'),
					        coalesce(IsForbesLargestPCL, 'IsForbesLargestPCL'),
					        coalesce(FamilyTreeRoles, 'FamilyTreeRoles'),
					        coalesce(MostSeniorPrincipals, 'MostSeniorPrincipals'),
					        coalesce(ControlOwnershipDate, 'ControlOwnershipDate')
						  )) AS MD5_hash,
		*
   FROM
		(select 'DUNS' as Identifier_type,
		 		dunsnumber as Identifier_ID,
		 		TRIM(' ~!@#$%^&*()_+-={},.?/' FROM primaryname) as Name,
		 		'' as Name1,
		 		'' as Name2,
		 		'' as Name3,
		 		'' as tax_number1,
		 		'' as tax_number2,
		 		'' as tax_number3,
		 		'' as tax_number4,
		 		'' as tax_number5,
		 		'' as tax_number6,
		 		registrationNumbers as VATregistrationnumber,
		 		operatingstatus AS StatusInformation_Status,
		        coalesce('', 'Active') as LegalStatus,
		        '' as StoreID,
		        '' as CompanyCode,
		        '' as CompanyCodeDescription,
		        '' as AccountGroup,
		        '' as AccountGroupDescription,
		        '' as CustomerClassification,
		        '' as CustomerClassificationDescription,
		        '' as IndustryCode,
		        '' as IndustryCodeDescription,
		        '' as Email_email,
		        '' AS Email_type,
		        primarytelephonenumber as Phone_number,
		        '' as Phone_Type,
		        '' as Fax_number,
		        '' as Franchise,
		        '' as FranchiseDescription,
		        '' as SalesOrg_SalesOrg,
		        '' as SalesOrg_SalesOrgStatus,
		        '' as CurrencySalesOrg,
		        dunsnumber as DunsNumber,
		        'Registered' as NameType,
		        registeredName as AlternativeName,
		        websiteAddress as WebsiteURL,
		        isStandalone as Standalone,
		        isFortune1000Listed as IsFortune1000Listed,
		        isForbesLargestPrivateCompaniesListed as IsForbesLargestPCL,
		        familytreeroles as FamilyTreeRoles,
		        mostseniorprincipals as MostSeniorPrincipals,
		        controlownershipdate as ControlOwnershipDate,
		         'configuration/sources/DNB' AS Crosswalk__type,
				dunsnumber AS crosswalk__value,
				current_date AS crosswalk__createdTime,
				'ebx_hierarchy_hierarchy_dunandbradstreetaccounts' AS crosswalk__sourceTable,
				'DNB_RAW_TO_TRUSTED' batch_id,
				current_date run_date
		   from adl_raw_gbl_cf_customer_mdm.ebx_hierarchy_hierarchy_dunandbradstreetaccounts  
		  )""")
df_address = spark.sql(""" SELECT MD5(CONCAT(Identifier_ID,COALESCE(Address_value_AddressLine1,'ADDR1'),
 			COALESCE(Address_value_AddressLine2,'ADDR2'),
 			COALESCE(Address_value_Zip,'POSTAL'),
 			COALESCE(Address_value_City,'CITY'),
 			--COALESCE(Address_value_Region,'REGION'),
 			COALESCE(StateProvince,'StateProvince'),
 			COALESCE(Address_value_COUNTRY,'COUNTRY'))) AS MD5_hash_addr,
 		*
   FROM
		(
		(select
				dunsnumber as Identifier_ID,
				primaryaddressline1 as Address_value_AddressLine1,
				primaryaddressline2 as Address_value_AddressLine2,
				'' as Address_value_AddressLine3,
				'' as Address_value_AddressLine4,
				primarypostalcode as Address_value_Zip,
				primarycity as Address_value_City,
				'' as Address_value_Region,
				'' as Address_value_City2,
				primaryregion as StateProvince,
				primarycountry as Address_value_Country,
				'configuration/entityTypes/Location' AS Address_refEntity_Type,
				'configuration/sources/FS_SAP_DC_CUST' as Address_refEntity_crosswalk_type,
				'surrogate' as Address_refEntity_crosswalk_value,
				'created_date' as Address_refEntity_crosswalk_createDate,
				'configuration/relationTypes/HCOHasAddress' as Address_refRelation_type,
				'configuration/sources/FS_SAP_DC_CUST' as Address_refRelation_crosswalk_type,
				'DNB'||coalesce(dunsnumber,'') || 'business_address_id' as Address_refRelation_crosswalk_value,
				'created_date' as Address_refRelation_crosswalk_createDate 
		   from adl_raw_gbl_cf_customer_mdm.ebx_hierarchy_hierarchy_dunandbradstreetaccounts  
		  )
		  union all
		(select
				dunsnumber as Identifier_ID,
				mailingaddressline1 as Address_value_AddressLine1,
				mailingaddressline2 as Address_value_AddressLine2,
				'' as Address_value_AddressLine3,
				'' as Address_value_AddressLine4,
				mailingpostalcode as Address_value_Zip,
				mailingcity as Address_value_City,
				'' as Address_value_Region,
				'' as Address_value_City2,
				mailingregion as StateProvince,
				mailingcountry as Address_value_Country,
				'configuration/entityTypes/Location' AS Address_refEntity_Type,
				'configuration/sources/FS_SAP_DC_CUST' as Address_refEntity_crosswalk_type,
				'surrogate' as Address_refEntity_crosswalk_value,
				'created_date' as Address_refEntity_crosswalk_createDate,
				'configuration/relationTypes/HCOHasAddress' as Address_refRelation_type,
				'configuration/sources/FS_SAP_DC_CUST' as Address_refRelation_crosswalk_type,
				'DNB'||coalesce(dunsnumber,'') || 'mailing_address_id' as Address_refRelation_crosswalk_value,
				'created_date' as Address_refRelation_crosswalk_createDate 
		   from adl_raw_gbl_cf_customer_mdm.ebx_hierarchy_hierarchy_dunandbradstreetaccounts  
		  ))""")
# S3 bucket path
s3_bucket = "s3://adl-base-customer-md-9b69xa1sw6t9jtkd3azyi3rjois1huse1b-s3alias/trusted/FS_DNB/DNB_customer/"
s3_bucket_ADDR = "s3://adl-base-customer-md-9b69xa1sw6t9jtkd3azyi3rjois1huse1b-s3alias/trusted/FS_DNB/DNB_Customer_adder/"
DNB_CUST_ADDR_COMB="s3://adl-base-customer-md-9b69xa1sw6t9jtkd3azyi3rjois1huse1b-s3alias/trusted/FS_DNB/DNB_CUST_ADDR_COMB"
# Write DataFrame to S3 in Parquet format
df.write.option("encoding", "UTF-8").mode("overwrite").parquet(s3_bucket)
df_address.write.option("encoding", "UTF-8").mode("overwrite").parquet(s3_bucket_ADDR)
print("Data successfully written")
dnb_customer_address_df = df.join(df_address, "Identifier_ID","left")
dnb_customer_address_df.write.option("encoding", "UTF-8").mode("overwrite").parquet(DNB_CUST_ADDR_COMB)