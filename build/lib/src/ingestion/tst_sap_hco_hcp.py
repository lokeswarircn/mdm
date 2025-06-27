from pyspark.sql import SparkSession
from pyspark.sql import Row

# Initialize SparkSession
spark = SparkSession.builder.appName("SparkDataframe").getOrCreate()

# Execute SQL query and create a DataFrame
df = spark.sql("""select md5(CONCAT(Identifier_type,Identifier_ID,
		coalesce(Name,'Name'),
		coalesce(LegalStatus,'LegalStatus'),
		StatusInformation_Status,
		coalesce(CompanyCode,'company_code'),
		coalesce(CompanyCodeDescription,'company_code_description'),
		coalesce(AccountGroup,'customer_account_group'),
		coalesce(AccountGroupDescription,'customer_account_group_description'),
		coalesce(CustomerClassification,'customer_classification'),
		coalesce(CustomerClassificationDescription,'customer_classification_description'),
		coalesce(IndustryCode,'industry_code'),
		coalesce(IndustryCodeDescription,'industry_description'),
		coalesce(Email_email,'email'),
		coalesce(Phone_number,'phone'),
		coalesce(Fax_number,'fax'), 
		coalesce(Franchise,'customer_franchise'),
		coalesce(FranchiseDescription,'customer_franchise_description'),
		coalesce(SalesOrg_SalesOrg,'sales_organization'), 
		coalesce(CurrencySalesOrg,'currency'),
		coalesce(CreatedDate,'created_date'),
		coalesce(CommunicationPreference_PreferredMethodofCommunication,'communication_method'))) AS MD5_hash_cust,
		*
   FROM
		(select 'SAP_ID' as Identifier_type,
		 		sap_commercial_account_id as Identifier_ID,
		 		TRIM(' ~!@#$%^&*()_+-={},.?/' FROM sap_commercial_account_name) as Name,
				'' as Title,
				'' as FirstName,
				'' as MiddleName,
				'' as LastName,
				'' as PrefferedName,
				'' as Education_TypeOfEducation,
				'' as IntegrationStatus,
				CASE WHEN central_delivery_block IS NOT NULL AND order_block IS NOT NULL AND billing_block IS NOT NULL AND deletion_flag_cust IS NOT NULL THEN 'Inactive'
		        	 ELSE 'Active' 
		         END AS StatusInformation_Status,
		        customer_legal_status_description as LegalStatus,
				'' as Gender,
				'' as AccountType,
				'' as GeographicRole,
				'' as ProfessionalType,
				'' as PracticeAtHospital,
				'' as ProfessionalType,
				company_code as CompanyCode,
		        company_code_description as CompanyCodeDescription,
				'' as CompCodeStatus,
				'' as CurrencyCompCode,
				customer_account_group as AccountGroup,
		        customer_account_group_description as AccountGroupDescription,
		        customer_classification as CustomerClassification,
		        customer_classification_description as CustomerClassificationDescription,
		        --IndustryCode as industry_code,
				industry_description as IndustryCodeDescription,
				'' as Gender,
				'' as Language,
				'' as Email,
				'' as Email_Type,
		        email as Email_email,'' as Domain,'' as Username,'' as Rank,'' as ValidationStatus,
		        '' as Phone,'' as Phone_Type,'' as Phone_Unreachable,'' as Phone_CountryCode,'' as Phone_Extension,'' as Phone_FormattedNumber,phone as Phone_number,'' as Phone_Rank,'' as Phone_AreaCode,'' as Phone_LocalNumber,
				'' as License,'' as License_State,'' as License_Number,'' as License_Category,'' as License_Category,'' as License_Type,'' as License_BoardCode,'' as License_Degree,'' as License_Status,'' as License_ExpirationDate,'' as License_IssueDate,'' as License_BrdDate,
		        fax as Fax_number,'' as WebsiteURL,
		        customer_franchise as Franchise,
		        customer_franchise_description as FranchiseDescription,
		        sales_organization as SalesOrg_SalesOrg,
				'' as SalesOrganizationDescription,'' as SOStatus,'' as AuthorizationCode,'' as AuthorizationDescription,'' as IntentionalDuplicate,
				'' as CommunicationPreference_PrivacyDoNotEmail,'' as CommunicationPreference_DoNotFax,'' as CommunicationPreference_DoNotSendPostalMails,'' as CommunicationPreference_DoNotPhoneCall,'' as LegacyCustomerSFDCID,'' as OwnershipStatus,
		        '' as SalesOrg_SalesOrgStatus,
		        currency as currency,
		        'configuration/sources/FS_SAP_DC_CUST' AS Crosswalk__type,
				sap_commercial_account_id AS crosswalk__value,
				created_date AS created_date,
				'dim_account' AS crosswalk__sourceTable,
				'HCP_SAP_RAW_TO_TRUSTED' batch_id,
				current_date as run_date
		   from adl_raw_gbl_cf_customer_mdm.dim_customer1 
		  WHERE Customer_Account_Group = '8C05');
""")
df1 = spark.sql(""" SELECT MD5(CONCAT(Identifier_ID,COALESCE(Address_value_AddressLine1,'ADDR1'),
 			COALESCE(Address_value_AddressLine1,'ADDR2'),
 			COALESCE(Address_value_Zip,'POSTAL'),
 			COALESCE(Address_value_City,'CITY'),
 			COALESCE(Address_value_Region,'REGION'),
 			COALESCE(Address_value_CITY2,'CITY_2'),
 			COALESCE(Address_value_COUNTRY,'COUNTRY'))) AS MD5_hash_add,
 		*
   FROM
                (select
		sap_commercial_account_id as Identifier_ID,
		sf_customer_id,
		address_line_1 as Address_value_AddressLine1,
		address_line_2 as Address_value_AddressLine2,
		address_line_3 as Address_value_AddressLine3,
		address_line_4 as Address_value_AddressLine4,
		postal as Address_value_Zip,
		city as Address_value_City,
		region as Address_value_Region,
		city_2 as Address_value_City2,
		country as Address_value_Country,
		'configuration/entityTypes/Location' AS Address_refEntity_Type,
		'configuration/sources/FS_SAP_DC_CUST' as Address_refEntity_crosswalk_type,
		'surrogate' as Address_refEntity_crosswalk_value,
		created_date as Address_refEntity_crosswalk_createDate,
		'configuration/relationTypes/HCOHasAddress' as Address_refRelation_type,
		'configuration/sources/FS_SAP_DC_CUST' as Address_refRelation_crosswalk_type,
		coalesce(sap_commercial_account_id,'') || 'address_id' as Address_refRelation_crosswalk_value,
		created_date as Address_refRelation_crosswalk_createDate 
   from adl_raw_gbl_cf_customer_mdm.dim_customer1
  where customer_account_group IN ('8C05'))""")
#HCO Customer
dfHCO_Customer=spark.sql("""select md5(CONCAT(Identifier_type,Identifier_ID,
		coalesce(Name,'Name'),coalesce(Name,'Name1'),coalesce(Name,'Name2'),coalesce(Name,'Name3'),tax_number4,tax_number5,
        coalesce(VATregistrationnumber,'VATregistrationnumber'),StatusInformation_Status,coalesce(LegalStatus,'Active'),
        coalesce(StoreID, 'store_number'),coalesce(CompanyCode,'company_code'),coalesce(CompanyCodeDescription,'company_code_description'),coalesce(AccountGroup,'customer_account_group'),
        coalesce(AccountGroupDescription,'customer_account_group_description'),coalesce(CustomerClassification,'customer_classification'),
		coalesce(CustomerClassificationDescription,'customer_classification_description'),coalesce(IndustryCode,'industry_code'),
		coalesce(IndustryCodeDescription,'industry_description'),coalesce(Email_email,'email'),coalesce(Phone_number,'phone'),coalesce(Fax_number,'fax'), 
		coalesce(Franchise,'customer_franchise'),coalesce(FranchiseDescription,'customer_franchise_description'),coalesce(SalesOrg_SalesOrg,'sales_organization'),      
		SalesOrg_SalesOrgStatus,        coalesce(CurrencySalesOrg,'currency')
		)) AS MD5_hash_cust,
		*
   FROM
		(select 'SAP_ID' as Identifier_type,
		 		sap_commercial_account_id as Identifier_ID,
		 		TRIM(' ~!@#$%^&*()_+-={},.?/' FROM sap_commercial_account_name) as Name,
		 		name1 as Name1,
		 		name2 as Name2,
		 		name3 as  Name3,
		 		tax_number4 as tax_number4,
		 		tax_number5 as tax_number5,
		 		vat_registration_number as VATregistrationnumber,
		 		CASE WHEN central_delivery_block IS NOT NULL AND order_block IS NOT NULL AND billing_block IS NOT NULL AND deletion_flag_cust IS NOT NULL THEN 'Inactive'
		        	 ELSE 'Active' 
		         END AS StatusInformation_Status,
		        customer_legal_status_description as LegalStatus,
		        store_number as StoreID,
		        company_code as CompanyCode,
		        company_code_description as CompanyCodeDescription,
		        customer_account_group as AccountGroup,
		        customer_account_group_description as AccountGroupDescription,
		        customer_classification as CustomerClassification,
		        customer_classification_description as CustomerClassificationDescription,
		        industry_code as IndustryCode,
		        industry_description as IndustryCodeDescription,
		        email as Email_email,
		        phone as Phone_number,
		        fax as Fax_number,
		        customer_franchise as Franchise,
		        customer_franchise_description as FranchiseDescription,
		        sales_organization as SalesOrg_SalesOrg,
		        '' as SalesOrg_SalesOrgStatus,
		        currency as CurrencySalesOrg,
		        'configuration/sources/FS_SAP_DC_CUST' AS Crosswalk__type,
				sap_commercial_account_id AS crosswalk__value,
				created_date AS crosswalk__createdTime,
				'dim_account' AS crosswalk__sourceTable,
				'HCP_SAP_RAW_TO_TRUSTED' batch_id,
				current_date as run_date
		   from adl_raw_gbl_cf_customer_mdm.dim_customer1 
		  WHERE Customer_Account_Group IN ('8C01', '8C02', '8C03' ,'8C04' , '8C06', '8C07', '8C08','8C0A', '8C13'))
""")
#HCO_Customer_Address
dfHCO_Customer_ADDR=spark.sql(""" SELECT MD5(CONCAT(Identifier_ID,COALESCE(Address_value_AddressLine1,'ADDR1'),
 			COALESCE(Address_value_AddressLine1,'ADDR2'),
 			COALESCE(Address_value_Zip,'POSTAL'),
 			COALESCE(Address_value_City,'CITY'),
 			COALESCE(Address_value_Region,'REGION'),
 			COALESCE(Address_value_CITY2,'CITY_2'),
 			COALESCE(Address_value_COUNTRY,'COUNTRY'))) AS MD5_hash_add,
 		*
   FROM
                (select
		sap_commercial_account_id as Identifier_ID,
		sf_customer_id,
		address_line_1 as Address_value_AddressLine1,
		address_line_2 as Address_value_AddressLine2,
		address_line_3 as Address_value_AddressLine3,
		address_line_4 as Address_value_AddressLine4,
		postal as Address_value_Zip,
		city as Address_value_City,
		region as Address_value_Region,
		city_2 as Address_value_City2,
		country as Address_value_Country,
		'configuration/entityTypes/Location' AS Address_refEntity_Type,
		'configuration/sources/FS_SAP_DC_CUST' as Address_refEntity_crosswalk_type,
		'surrogate' as Address_refEntity_crosswalk_value,
		created_date as Address_refEntity_crosswalk_createDate,
		'configuration/relationTypes/HCOHasAddress' as Address_refRelation_type,
		'configuration/sources/FS_SAP_DC_CUST' as Address_refRelation_crosswalk_type,
		coalesce(sap_commercial_account_id,'') || 'address_id' as Address_refRelation_crosswalk_value,
		created_date as Address_refRelation_crosswalk_createDate 
   from adl_raw_gbl_cf_customer_mdm.dim_customer1
  where customer_account_group IN ('8C01', '8C02', '8C03' ,'8C04' , '8C06', '8C07', '8C08','8C0A', '8C13'))""")
# S3 bucket path
s3_bucket = "s3://adl-base-customer-md-9b69xa1sw6t9jtkd3azyi3rjois1huse1b-s3alias/trusted/FS_SAP_DC_CUST/HCP_SAP_CUSTOMER"
s3_bucket_ADDR = "s3://adl-base-customer-md-9b69xa1sw6t9jtkd3azyi3rjois1huse1b-s3alias/trusted/FS_SAP_DC_CUST/HCP_SAP_CUSTOMER_ADDR"
s3_bucket_HCO_CUSTOMER="s3://adl-base-customer-md-9b69xa1sw6t9jtkd3azyi3rjois1huse1b-s3alias/trusted/FS_SAP_DC_CUST/HCO_SAP_CUSTOMER"
s3_bucket_HCO_CUSTOMER_ADDR="s3://adl-base-customer-md-9b69xa1sw6t9jtkd3azyi3rjois1huse1b-s3alias/trusted/FS_SAP_DC_CUST/ HCO_SAP_CUSTOMER_ADDR"
HCP_CUST_ADDR_COMB="s3://adl-base-customer-md-9b69xa1sw6t9jtkd3azyi3rjois1huse1b-s3alias/trusted/FS_SAP_DC_CUST/HCP_CUST_ADDR_COMB"
HCO_CUST_ADDR_COMB="s3://adl-base-customer-md-9b69xa1sw6t9jtkd3azyi3rjois1huse1b-s3alias/trusted/FS_SAP_DC_CUST/HCO_CUST_ADDR_COMB"
# Write DataFrame to S3 in Parquet format
df.write.option("encoding", "UTF-8").mode("overwrite").parquet(s3_bucket)
df1.write.option("encoding", "UTF-8").mode("overwrite").parquet(s3_bucket_ADDR)
dfHCO_Customer.write.option("encoding", "UTF-8").mode("overwrite").parquet(s3_bucket_HCO_CUSTOMER)
dfHCO_Customer_ADDR.write.option("encoding", "UTF-8").mode("overwrite").parquet(s3_bucket_HCO_CUSTOMER_ADDR)
print("Data successfully written to dim_customer")

hcp_customer_address_df = df.join(df1, "Identifier_ID","left")
hco_customer_address_df = dfHCO_Customer.join(dfHCO_Customer_ADDR,"Identifier_ID","left")



hcp_customer_address_df.write.option("encoding", "UTF-8").mode("overwrite").parquet(HCP_CUST_ADDR_COMB)
hco_customer_address_df.write.option("encoding", "UTF-8").mode("overwrite").parquet(HCO_CUST_ADDR_COMB)