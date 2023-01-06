---------------------------------------------------------------------------------
-- fact table: demographic cuts of users in network
---------------------------------------------------------------------------------

create or replace table zenalytics.business_profiles.business_users_demographics_summary_fact
as
WITH total_users as (select business_id, 
                     count(email) as total_users 
                     from zenalytics.business_profiles.business_users_details_fact udf 
                     group by 1),
demo_cuts as (
  select ac.business_id,
      ac.age,
      Initcap(ac.gender) as gender,
      ac.income,
      count(ac.email) as users
from  zenalytics.crm.analytics_customer ac
group by rollup(1,2,3,4)
order by business_id
)
select t.business_id, 
      total_users,
      age, 
      gender,
      income,
      users,
      current_date() as asof_date
from total_users t LEFT JOIN demo_cuts d  
on d.business_id = t.business_id;

