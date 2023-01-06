---------------------------------------------------------------
---------- DDL for AUDIENCES.audience
---------------------------------------------------------------

-- create a temp table to hold the business id,email tuples with the latest customer type tag, for non-employees
create or replace temporary table user_business_
as
select  customer_sk,
        business_id,
        FIRST_VALUE(created) over (partition by business_id, customer_sk order by created) as created,
        LAST_VALUE(created) over (partition by business_id, customer_sk order by created) as updated,
        LAST_VALUE(customer_type) over (partition by business_id, customer_sk order by created) as customer_type  -- pick the most recent customer type for any business as baseline
from zenalytics.crm.analytics_customer
where non_employee is Null or non_employee = True;

-- exlclude (business_id, customer_sk) pairs that are tagged in analytics_customer already
-- retain the records not in analytics_customer as "Passive Detection"
create or replace table user_sightings_
as
select business_id, customer_sk, 'PASSIVE_DETECTION' as customer_type, created, current_date() as asof_date
from zenalytics.audiences.user_sightings s
where not exists (select 'x' from zenalytics.audiences.user_business_ b where s.business_id = b.business_id and s.customer_sk = b.customer_sk)
;

-- union above with (business_id,email) sightings not found in analytics customer, these are passive detections
create or replace table zenalytics.audiences.audience
as
select * from
  (
  select business_id, customer_sk, customer_type, created, current_date() as asof_date from user_business_
  UNION ALL
  select business_id, customer_sk, 'PASSIVE_DETECTION' as customer_type, created, current_date() as asof_date from user_sightings_
  )
order by business_id, customer_sk;
