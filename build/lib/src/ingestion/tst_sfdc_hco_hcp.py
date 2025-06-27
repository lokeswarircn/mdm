from pyspark.sql import SparkSession
from pyspark.sql import Row


# Initialize SparkSession
spark = SparkSession.builder.appName("SparkDataframe").getOrCreate()
spark.conf.set("spark.sql.legacy.parquet.int96RebaseModeInWrite", "LEGACY")
# Execute SQL query and create a DataFrame
df = spark.sql(""" select md5(CONCAT(Identifier_type,Identifier_ID,
		Name,coalesce(Name1,'name1'),coalesce(Name2,'name2') ,coalesce(Name3,'Name3'),tax_number4,tax_number5,coalesce(VATregistrationnumber,'VATregistrationnumber'),StatusInformation_Status,
		coalesce(LegalStatus,'LegalStatus'),coalesce(StoreID,'StoreID'),CompanyCode,CompanyCodeDescription,AccountGroup,coalesce(AccountGroupDescription,'AccountGroupDescription'),CustomerClassification,coalesce(CustomerClassificationDescription,'CustomerClassificationDescription'),
		IndustryCode,coalesce(IndustryCodeDescription,'IndustryCodeDescription'),coalesce(Email_email,'Email_email'),coalesce(Phone_number,'Phone_number'),coalesce(Fax_number,'Fax_number'), Franchise, coalesce(FranchiseDescription,'FranchiseDescription'),coalesce(SalesOrg_SalesOrg,'SalesOrg_SalesOrg'),
		SalesOrg_SalesOrgStatus,coalesce(CurrencySalesOrg,'CurrencySalesOrg')
		)) AS MD5_hash,
		*
    FROM  
          (select 'SFDC_ID' as Identifier_type,
 		sf_id_ac__c as Identifier_ID,
 		TRIM(' ~!@#$%^&*()_+-={},.?/' FROM TRIM(CONCAT_WS(' ', name, name2_ac__c, Name_3_AC__c, Name_4_AC__c))) as Name,
 		name as Name1,
 		name2_ac__c as Name2,
 		name_3_ac__c as Name3,
 		'' as tax_number4,
 		'' as tax_number5,
 		'' as VATregistrationnumber,
 		status_ac__c AS StatusInformation_Status,
    legal_status_ac__c as LegalStatus,
        store_id_ac__c as StoreID,
        '' as CompanyCode,
        '' as CompanyCodeDescription,
        '' as AccountGroup,
        account_group_ac__c as AccountGroupDescription,
        '' as CustomerClassification,
        customer_classification_ac__c as CustomerClassificationDescription,
        '' as IndustryCode,
        industry_code_ac__c as IndustryCodeDescription,
        email_ac__c as Email_email,
        phone as Phone_number,
        fax as Fax_number,
        '' as Franchise,
        franchise_ac__c as FranchiseDescription,
        sales_org_ac__c as SalesOrg_SalesOrg,
        '' as SalesOrg_SalesOrgStatus,
        currencyisocode as CurrencySalesOrg,
        'configuration/sources/FS_SAP_DC_CUST' AS Crosswalk__type,
		sf_id_ac__c AS crosswalk__value,
		createddate AS crosswalk__createdTime,
		'dim_account' AS crosswalk__sourceTable,
		'HCP_SFDC_RAW_TRUSTED' batch_id,
		current_date as run_date
   from adl_raw_gbl_cf_customer_mdm.dim_account
  where recordtypeid = '012G00000016ciRIAQ')
""")
print("Debuging") 
df.show()
df.printSchema()
df1 = spark.sql("""select md5(CONCAT(Identifier_type,Identifier_ID,
		coalesce(Name,'Name'),
		coalesce(Title,'salutation'),
		coalesce(FirstName,'FirstName'),
		coalesce(MiddleName,'middlename'),
		coalesce(LastName,'lastname'),
		coalesce(PrefferedName,'PrefferedName'),
		coalesce(LegalStatus,'LegalStatus'),
		coalesce(IntegrationStatus,'IntegrationStatus'),
		StatusInformation_Status,
		coalesce(Gender,'Gender'),
		coalesce(AccountType,'AccountType'),
		coalesce(GeographicRole,'GeographicRole'),
		coalesce(ProfessionalType,'ProfessionalType'),
		coalesce(PracticeAtHospital,'PracticeAtHospital'),
		coalesce(GeographicRole,'GeographicRole'),
		coalesce(AccountGroupDescription,'customer_account_group_description'),
		coalesce(CustomerClassification,'customer_classification'),
		coalesce(IndustryCodeDescription,'industry_description'),
		coalesce(Language,'Language'),
		coalesce(Email_email,'email'),
		coalesce(Phone_number,'phone'),
		coalesce(Fax_number,'fax'), 
		coalesce(WebsiteURL,'website'), 
		coalesce(Franchise,'customer_franchise'),
		coalesce(SalesOrg_SalesOrg,'sales_organization'), 
		coalesce(CurrencySalesOrg,'currency'),
		coalesce(CreatedDate,'created_date'),
		coalesce(CommunicationPreference_PreferredMethodofCommunication,'communication_method'),
		coalesce(CommunicationPreference_PrivacyDoNotEmail,'PrivacyDoNotEmail'),
		coalesce(CommunicationPreference_DoNotFax,'DoNotFax'),
		coalesce(CommunicationPreference_DoNotSendPostalMails,'DoNotSendPostalMails'),
		coalesce(CommunicationPreference_DoNotPhoneCall,'DoNotPhoneCall'),
		coalesce(LegacyCustomerSFDCID,'LegacyCustomerSFDCID'),
		coalesce(OwnershipStatus,'OwnershipStatus'))) AS MD5_hash,
		*
    FROM  
          (select 'SFDC_ID' as Identifier_type,
 		sf_id_ac__c as Identifier_ID,
		 		--TRIM(' ~!@#$%^&*()_+-={},.?/' FROM TRIM(CONCAT_WS(' ', name) as Name, 	
				FirstName as FirstName,
				MiddleName as MiddleName,
				LastName as LastName,
				'' as PrefferedName,
				'' as Education_TypeOfEducation,
				legal_status_ac__c as LegalStatus,
				status_ac__c as StatusInformation_Status,
				gender_vod__c as Gender,
				account_type__c as AccountType,
				key_account__c as GeographicRole,
				professional_type_ac__c as ProfessionalType,
				practice_at_hospital_vod__c as PracticeAtHospital,
				'' as CompanyCode,
		        '' as CompanyCodeDescription,
				'' as CompCodeStatus,
				'' as CurrencyCompCode,
				'' as AccountGroup,
				account_group_ac__c as AccountGroupDescription,
				customer_classification_ac__c as CustomerClassification,
				'' as IndustryCode,
				industry_code_ac__c as IndustryCodeDescription,
				language_ac__c as Language,
				'' as Email,
				'' as Email_Type,
				email_ac__c as Email_email,'' as Domain,'' as Username,'' as Rank,'' as ValidationStatus,
				'' as Phone,'' as Phone_Type,'' as Phone_Unreachable,'' as Phone_CountryCode,'' as Phone_Extension,'' as Phone_FormattedNumber,phone as Phone_number,'' as Phone_Rank,'' as Phone_AreaCode,'' as Phone_LocalNumber,
				fax as fax,
				website as WebsiteURL,
				franchise_ac__c as Franchise,
				'' as FranchiseDescription,
				'' as License,'' as License_State,'' as License_Number,'' as License_Category,'' as License_Category,'' as License_Type,'' as License_BoardCode,'' as License_Degree,'' as License_Status,'' as License_ExpirationDate,'' as License_IssueDate,'' as License_BrdCode,
				sales_org_ac__c as SalesOrg_SalesOrg,
				'' as SalesOrganizationDescription,'' as SOStatus,'' as AuthorizationCode,'' as AuthorizationDescription,'' as IntentionalDuplicate,
				currencyisocode as currency,
				--created_on as CreatedDate,
				'configuration/sources/FS_SFDC_DC_CUST' AS Crosswalk__type,
				coalesce(sf_id_ac__c,'') AS crosswalk__value,
		createddate AS crosswalk__createdTime,
		'dim_account' AS crosswalk__sourceTable,
		'HCO_SFDC_RAW_TRUSTED' batch_id,
		current_date as run_date
   from adl_raw_gbl_cf_customer_mdm.dim_account
  where recordtypeid = '012G00000019HNJIA2')""")
#HCP address
df2 = spark.sql("""select md5(CONCAT(coalesce(Identifier_ID,'Identifier_ID'),coalesce(SFDCIDL,'SFDCIDL'),coalesce(SFDCIDS,'SFDCIDS'),coalesce(PrimaryAddressme,'PrimaryAddressme'),coalesce(AddressLine1,'AddressLine1'),coalesce(AddressLine2,'AddressLine2'),coalesce(City,'City'),
coalesce(PostalCode,'PostalCode'),coalesce(Region,'Region'),coalesce(Country,'Country'),coalesce(Address_type,'Address_type'),coalesce(AccountVod,'AccountVod'))) as MD5_hash1,
*
from
(select
sf_id_ac__c as Identifier_ID,
id as SFDCIDL,
sf_id_ac__c as SFDCIDS,
'null' as PrimaryAddressme,
house_and_street_ac__c as AddressLine1,
'null' as AddressLine2,
city_ac__c as City,
primary_address_zip_code_ac__c as PostalCode,
primary_address_region_ac__c as Region,
country_code_ac__c as Country,
'Primary' Address_type,
'null' as AccountVod
from adl_raw_gbl_cf_customer_mdm.dim_account
where country_code_ac__c = 'US'
and recordtypeid ='012G00000016ciRIAQ')
union
select md5(CONCAT(coalesce(Identifier_ID,'Identifier_ID'),coalesce(SFDCIDL,'SFDCIDL'),coalesce(SFDCIDS,'SFDCIDS'),coalesce(PrimaryAddressme,'PrimaryAddressme'),coalesce(AddressLine1,'AddressLine1'),coalesce(AddressLine2,'AddressLine2'),coalesce(City,'City'),
coalesce(PostalCode,'PostalCode'),coalesce(Region,'Region'),coalesce(Country,'Country'),coalesce(Address_type,'Address_type'),coalesce(AccountVod,'AccountVod'))) as MD5_hash1,
*
from
(select coalesce(act.sf_id_ac__c,'sf_id_ac__c') as Identifier_ID,
act.id as SFDCIDL,
act.sf_id_ac__c as SFDCIDS,
    CASE 
        WHEN addr.primary_vod__c THEN 'TRUE'
        ELSE 'FALSE'
    END AS PrimaryAddressme,
addr.name as AddressLine1,
addr.address_line_2_vod__c as AddressLine2,
addr.city_vod__c as City,
addr.zip_vod__c as PostalCode,
addr.region_ac__c as Region,
addr.country_vod__c as Country,
case
when primary_vod__c = True then 'Primary'
when primary_vod__c = False then 'Other'
end as Address_type,
addr.account_vod__c as AccountVod
from adl_raw_gbl_cf_customer_mdm.dim_address_vod__c addr,
adl_raw_gbl_cf_customer_mdm.dim_account act
where act.id = addr.account_vod__c
and addr.country_vod__c = 'US'
and act.recordtypeid ='012G00000016ciRIAQ')""")
#HCO_ADDR
df3 = spark.sql("""select md5(CONCAT(coalesce(Identifier_ID,'Identifier_ID'),coalesce(SFDCIDL,'SFDCIDL'),coalesce(SFDCIDS,'SFDCIDS'),coalesce(PrimaryAddressme,'PrimaryAddressme'),coalesce(AddressLine1,'AddressLine1'),coalesce(AddressLine2,'AddressLine2'),coalesce(City,'City'),
coalesce(PostalCode,'PostalCode'),coalesce(Region,'Region'),coalesce(Country,'Country'),coalesce(Address_type,'Address_type'),coalesce(AccountVod,'AccountVod'))) as MD5_hash1,
*
from
(select
sf_id_ac__c as Identifier_ID,
id as SFDCIDL,
sf_id_ac__c as SFDCIDS,
'null' as PrimaryAddressme,
house_and_street_ac__c as AddressLine1,
'null' as AddressLine2,
city_ac__c as City,
primary_address_zip_code_ac__c as PostalCode,
primary_address_region_ac__c as Region,
country_code_ac__c as Country,
'Primary' Address_type,
'null' as AccountVod
from adl_raw_gbl_cf_customer_mdm.dim_account
where country_code_ac__c = 'US'
and recordtypeid ='012G00000019HNJIA2')
union
select md5(CONCAT(coalesce(Identifier_ID,'Identifier_ID'),coalesce(SFDCIDL,'SFDCIDL'),coalesce(SFDCIDS,'SFDCIDS'),coalesce(PrimaryAddressme,'PrimaryAddressme'),coalesce(AddressLine1,'AddressLine1'),coalesce(AddressLine2,'AddressLine2'),coalesce(City,'City'),
coalesce(PostalCode,'PostalCode'),coalesce(Region,'Region'),coalesce(Country,'Country'),coalesce(Address_type,'Address_type'),coalesce(AccountVod,'AccountVod'))) as MD5_hash1,
*
from
(select coalesce(act.sf_id_ac__c,'sf_id_ac__c') as Identifier_ID,
act.id as SFDCIDL,
act.sf_id_ac__c as SFDCIDS,
    CASE 
        WHEN addr.primary_vod__c THEN 'TRUE'
        ELSE 'FALSE'
    END AS PrimaryAddressme,
addr.name as AddressLine1,
addr.address_line_2_vod__c as AddressLine2,
addr.city_vod__c as City,
addr.zip_vod__c as PostalCode,
addr.region_ac__c as Region,
addr.country_vod__c as Country,
case
when primary_vod__c = True then 'Primary'
when primary_vod__c = False then 'Other'
end as Address_type,
addr.account_vod__c as AccountVod
from adl_raw_gbl_cf_customer_mdm.dim_address_vod__c addr,
adl_raw_gbl_cf_customer_mdm.dim_account act
where act.id = addr.account_vod__c
and addr.country_vod__c = 'US'
and act.recordtypeid ='012G00000019HNJIA2')""")   
# S3 bucket path
s3_bucket = "s3://adl-base-customer-md-9b69xa1sw6t9jtkd3azyi3rjois1huse1b-s3alias/trusted/FS_SFDC_ACC_CUST/HCP_SFDC_CUSTOMER"
s3_bucket_HCO = "s3://adl-base-customer-md-9b69xa1sw6t9jtkd3azyi3rjois1huse1b-s3alias/trusted/FS_SFDC_ACC_CUST/HCO_SFDC_CUSTOMER"
s3_bucket_ADDR = "s3://adl-base-customer-md-9b69xa1sw6t9jtkd3azyi3rjois1huse1b-s3alias/trusted/FS_SFDC_ACC_CUST/HCP_SFDC_CUSTOMER_ADDR"
s3_bucket_HCO_ADDR = "s3://adl-base-customer-md-9b69xa1sw6t9jtkd3azyi3rjois1huse1b-s3alias/trusted/FS_SFDC_ACC_CUST/HCO_SFDC_CUSTOMER_ADDR"

HCP_CUST_ADDR_COMB="s3://adl-base-customer-md-9b69xa1sw6t9jtkd3azyi3rjois1huse1b-s3alias/trusted/FS_SFDC_ACC_CUST/HCP_comb"
HCO_CUST_ADDR_COMB="s3://adl-base-customer-md-9b69xa1sw6t9jtkd3azyi3rjois1huse1b-s3alias/trusted/FS_SFDC_ACC_CUST/HCO_comb"

# Write DataFrame to S3 in Parquet format
df.write.option("encoding", "UTF-8").mode("overwrite").parquet(s3_bucket)
df1.write.option("encoding", "UTF-8").mode("overwrite").parquet(s3_bucket_HCO)
df2.write.option("encoding", "UTF-8").mode("overwrite").parquet(s3_bucket_ADDR)
df3.write.option("encoding", "UTF-8").mode("overwrite").parquet(s3_bucket_HCO_ADDR)
print("Data successfully written")

# read csv and join with hcp_customer_address_df
# df->hcp df1->hco

# csv_hcp.join(df)

# csv_hco.join(df1)

# hcp - no address- so build a table on top of csv and dump it to sfdc_hc
# hco caution with address
hcp_customer_address_df = df.join(df2, "Identifier_ID","left")
hco_customer_address_df = df1.join(df3,"Identifier_ID","left")



hcp_customer_address_df.write.option("encoding", "UTF-8").mode("overwrite").parquet(HCP_CUST_ADDR_COMB)
hco_customer_address_df.write.option("encoding", "UTF-8").mode("overwrite").parquet(HCO_CUST_ADDR_COMB)