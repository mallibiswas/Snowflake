---------------------------------------------------------------------------------
--- ZR user network by business id
---------------------------------------------------------------------------------

create or replace table zenalytics.business_profiles.business_users_summary_fact
as 
select business_id, 
count(email) as users,
current_date as asof_date
from zenalytics.business_profiles.business_users_details_fact
group by 1;

