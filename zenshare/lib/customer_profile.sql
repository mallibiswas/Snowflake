-----------------------------------------------------------------------
---------------------  ZENSHARE.MAIN.VISITS_SMRY ----------------------
-----------------------------------------------------------------------

use role &{rolename};
use warehouse &{whname};
use database &{targetdb};
use schema &{targetschema};

alter session set QUOTED_IDENTIFIERS_IGNORE_CASE = false;
alter session set timezone = 'UTC';

/*
-- Main DDL
create or replace table &{targetdb}.&{targetschema}.customer_profile 
as
create or replace table zenshare.main.customer_profile
as
 select 
   bph.parent_id,
   br.business_id,
   up.userprofile_id as contact_id,   
   up.email,  
   age,
   gender,
   income,
   NVL(br.contact_allowed,TRUE) as is_subscribed, -- ? 
   NVL(br.is_employee,FALSE) as is_employee,
   tags,
   fullname as name,
   case when birthday_day is not null then birthday_day||'/'||birthday_month else null end as birthday,
   facebookprofile_id as facebook_id,
   city||','||state||','||zip_code as  address,
   not(NVL(ac.EMAIL_IS_VALID,TRUE)) as invalid,
   up.date_added as contact_created_date,
   up.date_added as create_dttm,
   up.date_added as update_dttm
  from zenshare.main.portal_userprofile up
  inner join zenshare.main.portal_businessrelationship br on up.userprofile_id = br.userprofile_id
  inner join zenshare.main.businessprofile_hierarchy bph on br.business_id = bph.business_id
  left join zenalytics.crm.analytics_customer ac on ac.email = up.email and ac.business_id = br.business_id
  where contains(up.email, '@') = True
  and exists (select 'x' from zenshare.main.portal_accessdeviceownership ado where up.userprofile_id = ado.userprofile_id)
  ;
*/

-- Incremental load
create or replace temporary table _customer_profile_
as
 select 
   bph.parent_id,
   br.business_id,
   up.userprofile_id as contact_id,   
   up.email,  
   age,
   gender,
   income,
   NVL(br.contact_allowed,TRUE) as is_subscribed, 
   NVL(br.is_employee,FALSE) as is_employee,
   tags,
   fullname as name,
   case when birthday_day is not null then birthday_day||'/'||birthday_month else null end as birthday,
   facebookprofile_id as facebook_id,
   city||','||state||','||zip_code as  address,
   not(NVL(ac.EMAIL_IS_VALID,TRUE)) as invalid,
   up.date_added as contact_created_date,
   up.date_added as create_dttm,
   up.date_added as update_dttm
  from &{targetdb}.&{targetschema}.portal_userprofile up
  inner join &{targetdb}.&{targetschema}.portal_businessrelationship br on up.userprofile_id = br.userprofile_id
  inner join &{targetdb}.&{targetschema}.businessprofile_hierarchy bph on br.business_id = bph.business_id
  left join &{analyticsdb}.&{analyticsschema}.analytics_customer ac on ac.email = up.email and ac.business_id = br.business_id
  where contains(up.email, '@') = True
  and up.date_added >= dateadd(day,-2,current_date) -- check last 2 days for load
  and exists (select 'x' from &{targetdb}.&{targetschema}.portal_accessdeviceownership ado where up.userprofile_id = ado.userprofile_id)
  ;

  MERGE into &{targetdb}.&{targetschema}.customer_profile tgt 
  using _customer_profile_ src on src.business_id = tgt.business_id and src.contact_id = tgt.contact_id
  WHEN not matched then 
    insert (PARENT_ID,
            BUSINESS_ID,
            CONTACT_ID,
            EMAIL,
            AGE,
            GENDER,
            INCOME,
            IS_SUBSCRIBED,
            IS_EMPLOYEE,
            TAGS,
            NAME,
            BIRTHDAY,
            FACEBOOK_ID,
            ADDRESS,
            INVALID,
            CONTACT_CREATED_DATE,
            CREATE_DTTM,
            UPDATE_DTTM) 
    values (src.PARENT_ID,
            src.BUSINESS_ID,
            src.CONTACT_ID,
            src.EMAIL,
            src.AGE,
            src.GENDER,
            src.INCOME,
            src.IS_SUBSCRIBED,
            src.IS_EMPLOYEE,
            src.TAGS,
            src.NAME,
            src.BIRTHDAY,
            src.FACEBOOK_ID,
            src.ADDRESS,
            src.INVALID,
            src.CONTACT_CREATED_DATE,
            src.CREATE_DTTM,
            src.UPDATE_DTTM)
            ;



