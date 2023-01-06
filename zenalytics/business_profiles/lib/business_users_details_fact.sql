-------------------------------------------------------------------------------
-- ZR User Network
-------------------------------------------------------------------------------

create or replace table zenalytics.business_profiles.business_users_details_fact
as
WITH email_business_map as 
(select lower(trim(ac.email)) as email, 
       ac.business_id, 
       rank() over (partition by email order by created, ac.business_id) as rnk
from zenalytics.crm.analytics_customer ac
),
userprofiles_with_mac as (
select 
  distinct lower(trim(up.email)) as email,
  up.email_is_valid,
  up.date_added,
  up.userprofile_id,
  up.importer_id
from zenalytics.crm.portal_userprofile up
inner join zenalytics.crm.portal_accessdeviceownership ado 
    on up.userprofile_id = ado.userprofile_id
where date_added >= '2013-01-01'  
and email_is_valid is not Null
and contains(up.email, '@') = True
)
select um.email,
       um.email_is_valid,
       um.date_added,
       um.userprofile_id,
       um.importer_id,
       ebm.business_id,
       current_date as asof_date
from email_business_map ebm, userprofiles_with_mac um
where ebm.email=um.email
and rnk = 1;

