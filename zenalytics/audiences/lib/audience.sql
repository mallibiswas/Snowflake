---------------------------------------------------------------
---------- Upsert for AUDIENCES.audience
---------------------------------------------------------------

ALTER SESSION SET QUOTED_IDENTIFIERS_IGNORE_CASE = FALSE;
ALTER SESSION SET TIMEZONE = 'UTC';

use warehouse &{whname};
use database &{tgtdbname};
use schema &{tgtschemaname};
use role &{rolename};

SET MIN_END_TS1 = (select max(created) from &{tgtdbname}.&{tgtschemaname}.audience where customer_type = 'PASSIVE_DETECTION');
SET MIN_END_TS2 = (select max(created) from &{tgtdbname}.&{tgtschemaname}.audience where customer_type <> 'PASSIVE_DETECTION');

SET MAX_END_TS = (select dateadd(day,-1,to_timestamp_ntz(current_date())));

-- create a temp table to hold the business id,email tuples with the latest customer type tag, for non-employees
create or replace temporary table user_business_
as
select  customer_sk,
        business_id,
        FIRST_VALUE(created) over (partition by business_id, customer_sk order by created)::timestamp_ntz as created,
        LAST_VALUE(created) over (partition by business_id, customer_sk order by created)::timestamp_ntz as updated,
        LAST_VALUE(customer_type) over (partition by business_id, customer_sk order by created) as customer_type  -- pick the most recent customer type for any business as baseline
from &{srcdbname}.&{srcschemaname}.analytics_customer
where (non_employee is Null or non_employee = True)
and created > to_timestamp_ntz($MIN_END_TS2) and created <= to_timestamp_ntz($MAX_END_TS)
;

create or replace temporary table user_sightings_
as
select business_id,
customer_sk,
'PASSIVE_DETECTION' as customer_type,
created,
current_date() as asof_date
from &{tgtdbname}.&{tgtschemaname}.user_sightings s
where created > to_timestamp_ntz($MIN_END_TS1) and created <= to_timestamp_ntz($MAX_END_TS)
and not exists (select 'x' from user_business_ b where s.business_id = b.business_id and s.customer_sk = b.customer_sk)
;

-- union above with (business_id,email) sightings not found in analytics customer, these are passive detections
create or replace temporary table audience_
as
select * from
  (
  select business_id, customer_sk, customer_type, created, current_date() as asof_date from user_business_
  UNION ALL
  select business_id, customer_sk, 'PASSIVE_DETECTION' as customer_type, created, current_date() as asof_date from user_sightings_
  )
order by business_id, customer_sk;

insert into &{tgtdbname}.&{tgtschemaname}.audience (business_id, customer_sk, customer_type, created, asof_date)
select src.business_id, src.customer_sk, src.customer_type, src.created, src.asof_date
from audience_ src;
