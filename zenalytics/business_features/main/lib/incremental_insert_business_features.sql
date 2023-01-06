-----------------------------------------------------------------------
---------------------  Business_Features_Raw ---------------------------
-----------------------------------------------------------------------

use database &{dbname};
use warehouse &{whname};
use schema &{schemaname};
use role &{rolename};

alter session set QUOTED_IDENTIFIERS_IGNORE_CASE = false;

-- stage data for processing
insert into &{tablename} (parent_id, parent_address, business_id, address, valid_rec, processed, manual_review, parent_name_orig, business_name_orig, created)
select  l1_id as parent_id, 
        l1_address as parent_address, 
        business_id, 
        address,
        True::boolean as valid_rec,
        False::boolean as processed,
        False::boolean as manual_review,
        l1_name as parent_name_orig, 
        business_name as business_name_orig, 
        convert_timezone('UTC',current_timestamp())::TIMESTAMP_NTZ as created
from &{dbname}.CRM.businessprofile_hierarchy
where business_id not in (select business_id from &{dbname}.&{schemaname}.&{tablename});

-- identify parent names with BS keywords and set to invalid rec
update &{tablename} set valid_rec = False 
where regexp_like(parent_name_orig, '(.*)(fake|bogus|test|demo|bad account|Macdonalds|duplicate|playground|Peets Network|Anheuser Busch InBev|ZR Concepts|AdamBomb|duplicate|Waterloo Meraki Prod)(.*)','i') 
	and valid_rec = True and processed = False;
-- identify business names with BS keywords and set to invalid rec
update &{tablename} set valid_rec = False 
where regexp_like(business_name_orig, '(.*)(fake|bogus|test|demo|bad account|Macdonalds|duplicate|playground|Peets Network|Anheuser Busch InBev|ZR Concepts|AdamBomb|duplicate|Waterloo Meraki Prod)(.*)','i') 
	and valid_rec = True and processed = False;
-- clean up special characters and punctuations from parent name 
update &{tablename} set parent_name = REGEXP_REPLACE(parent_name_orig,'[^a-zA-Z0-9 ]+',' ',1,0,'i') 
	where parent_name is null and processed = False;
-- clean up special characters and punctuations from business name 
update &{tablename} set business_name = REGEXP_REPLACE(business_name_orig,'[^a-zA-Z0-9 ]+',' ',1,0,'i') 
	where business_name is null and processed = False;
-- clean up useless tags/keywords from parent name 
update &{tablename} set parent_name = REGEXP_REPLACE(parent_name,'(parent|group|Corporate|llc)','',1,0,'i') 
	where valid_rec = True and processed = False;
-- clean up useless tags/keywords from business name 
update &{tablename} set business_name = REGEXP_REPLACE(business_name,'(parent|group|Corporate|llc)','',1,0,'i') 
	where valid_rec = True and processed = False;
