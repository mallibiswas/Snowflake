----------------------------------------------------------
------ order lifecycle dimension
----------------------------------------------------------

alter session set QUOTED_IDENTIFIERS_IGNORE_CASE = false;
alter session set TIMEZONE = 'UTC';

use database &{dbname};
use warehouse &{whname};
use role &{rolename};
use schema &{stageschemaname};

SET ASOF_DATE = (SELECT Current_Date());

-- #0 get all historical order cancel dates, this is an additional check to get accurate cancel dates from event logs
-- avoid pulling in migrated records
create or replace temporary table _all_order_cancelled_dates
as
  select    'v2' as version, 
            account_id, 
            contract_id, 
            entity_id as subscription_id, 
            parse_json(payload):new_state::string as state, 
            event_time as order_cancelled_date
  from &{sourcedb}.AMS.event
  where entity_name in ('contract_v2','subscription_v2') and event_name = 'STATE'
  and parse_json(payload):new_state = 'CANCELLED'
  UNION ALL
  select    'v1' as version, 
            account_id, 
            contract_id, 
            entity_id as subscription_id, 
            'CANCELLED' as state, 
            event_time as order_cancelled_date
  from &{sourcedb}.AMS.event
  where entity_name in ('contract','subscription') and event_name = 'STATE'
  and parse_json(payload):new_state = 'CLOSED'
  and contract_id not in (select contract_v1_id from &{sourcedb}.AMS.subscription_v2_migration);  

-- create recurly table
create or replace temporary table all_recurly_subscriptions_
as
select
    rs.uuid as recurly_subscription_id,
    nvl(rs.collection_method,'manual') as collection_method,
    rs.activated_at as subscription_activation_date,
    trim(parse_json(rs.plan):plan_code)::string as plan_code,
    rs.quantity as recurly_location_count,
    rs.unit_amount_in_cents as recurly_unit_amount_in_cents
from &{sourcedb}.RECURLY_V1.recurly_subscriptions rs
where state <> 'future'; 


create or replace temporary table _all_recurly_subscriptions
as
select  a.recurly_subscription_id, 
        a.collection_method,
        subscription_activation_date, 
        recurly_location_count, 
        p.name as recurly_plan_name, 
recurly_unit_amount_in_cents/plan_interval_length as recurly_monthly_subscription_service_fee
from all_recurly_subscriptions_ a,
     &{sourcedb}.RECURLY_V1.recurly_plans p
where a.plan_code = p.plan_code;


--- # V1 : Order lifecycle: base V1 contracts and subscriptions 
create or replace temporary table _d_order_lifecycle_V1
as
select a.contract_id, 
      trim('v1'||':'||a.contract_id) as contract_sk,
      a.account_id, 
      a.subscription_id,
      trim('v1'||':'||a.subscription_id) as subscription_sk,
      a.salesforce_opportunity_id,
      a.billing_account_id,
      0 as plan_id,
      a.payment_term,
      b.recurly_subscription_token as recurly_subscription_id,
      a.sent as contract_sent_date, 
      a.signed as contract_signed_date,
      a.contract_state as contract_current_state,
      DATE_TRUNC('SECOND',a.updated) as contract_cancelled_date, -- all contracts in this table are closed
      a.effective_date as subscription_start_date,
      a.pilot_length as trial_length,
      wait_period,
      dateadd(day,nvl(pilot_length,0)+nvl(wait_period,0),a.effective_date) as absolute_trial_end_date,
      rs.subscription_activation_date as subscription_activation_date,
      b.updated as subscription_cancelled_date, -- all subscriptions here are closed
      subscription_state as subscription_current_state,      
      rs.collection_method,
      a.service_fee/(case   when payment_term = 'MONTHLY' then 1 
                            when payment_term = 'QUARTERLY' then 3 
                            when payment_term = 'ANNUALLY' then 12 
                            else 1 end) as ams_monthly_subscription_service_fee,        
      recurly_monthly_subscription_service_fee,
      recurly_location_count, 
      recurly_plan_name, 
      null::timestamp as order_cancelled_date
from  &{sourcedb}.AMS.contract a 
inner join &{sourcedb}.AMS.account ac on a.account_id = ac.account_id
inner join &{sourcedb}.AMS.subscription b on a.account_id = b.account_id and a.subscription_id = b.subscription_id
left join _all_recurly_subscriptions rs on b.recurly_subscription_token = rs.recurly_subscription_id
where contract_state = 'CLOSED' 
-- and b.subscription_state = 'CLOSED' -- the migration was based on contracts, so a closed V1 contract = migrated contract
and a.beta = False
and ac.is_test = False
and a.contract_id not in (select contract_v1_id from &{sourcedb}.AMS.subscription_v2_migration);  -- avoid pulling in migrated records

-- updated migrated contracts which were closed 
update _d_order_lifecycle_V1 a
set contract_cancelled_date = b.created -- = migration date
from &{sourcedb}.AMS.subscription_v2_migration b
where a.contract_id = b.contract_v1_id;


--- # V2 : Order lifecycle: base V2 contracts and subscriptions 
create or replace temporary table _d_order_lifecycle_V2
as
select a.contract_id, 
      trim('v2'||':'||a.contract_id) as contract_sk,
      a.account_id, 
      b.subscription_id,
      trim('v2'||':'||b.subscription_id) as subscription_sk,
      a.salesforce_opportunity_id,
      b.billing_account_id,
      b.plan_id,
      p.payment_term,
      b.recurly_subscription_id,
      a.sent_date as contract_sent_date, 
      a.signed_date as contract_signed_date,
      a.contract_state as contract_current_state,
      case when a.contract_state = 'CANCELLED' then a.updated else null end as contract_cancelled_date,
      b.subscription_start_date as subscription_start_date,
      trial_length,
      wait_period,
      nvl(absolute_trial_end_date,dateadd(day,nvl(trial_length,0)+nvl(wait_period,0),subscription_start_date)) as absolute_trial_end_date,
      rs.subscription_activation_date as subscription_activation_date,
      case when b.subscription_state = 'CANCELLED' then DATE_TRUNC('SECOND', b.updated) else null end as subscription_cancelled_date,
      subscription_state as subscription_current_state,
      rs.collection_method,
      b.service_fee/(case   when payment_term = 'MONTHLY' then 1 
                            when payment_term = 'QUARTERLY' then 3 
                            when payment_term = 'ANNUALLY' then 12 
                            else 1 end) as ams_monthly_subscription_service_fee,        
      recurly_monthly_subscription_service_fee,
      recurly_location_count, 
      recurly_plan_name, 
      null::timestamp as order_cancelled_date
from &{sourcedb}.AMS.contract_v2 a 
inner join &{sourcedb}.AMS.account ac on a.account_id = ac.account_id
-- change: freeze subscriptions to pre-migration record in ams.subscriptions_v2
inner join &{sourcedb}.AMS.subscriptions_v2_pre_migration b on a.account_id = b.account_id and a.contract_id = b.contract_id
inner join &{sourcedb}.AMS.plan p on b.plan_id = p.plan_id
left join _all_recurly_subscriptions rs on b.recurly_subscription_id = rs.recurly_subscription_id
where ac.is_test = False
;

-- # create final order lifecycle table
create or replace table d_order_lifecycle
as 
select 
'v1' as version,
True::boolean as include_in_reporting,
v1.*,
nvl(ams_monthly_subscription_service_fee,recurly_monthly_subscription_service_fee) as monthly_subscription_service_fee
from _d_order_lifecycle_V1 v1 
UNION ALL
select 
'v2' as version,
True::boolean as include_in_reporting,
v2.*,
nvl(ams_monthly_subscription_service_fee,recurly_monthly_subscription_service_fee) as monthly_subscription_service_fee
from _d_order_lifecycle_V2 v2 
;

-- update all order cancelled dates, this is a way to get more accurate cancelled dates from event logs
update d_order_lifecycle a
set order_cancelled_date = nvl(b.order_cancelled_date, a.contract_cancelled_date)
from _all_order_cancelled_dates b
where a.version = b.version 
and a.account_id = b.account_id
and a.contract_id = b.contract_id
and a.subscription_id = b.subscription_id;

-- update subscriptions where we cannot track the subscription face amount 
-- or contract has been cancelled within 5 mins of creation
update d_order_lifecycle 
set include_in_reporting = False 
where nvl(monthly_subscription_service_fee,0) = 0 
or to_date(contract_sent_date) = to_date(contract_cancelled_date)
or to_date(subscription_start_date)=to_date(subscription_cancelled_date);

alter table d_order_lifecycle add column asof_date date;
update d_order_lifecycle set asof_date = $ASOF_DATE;

alter table &{stageschemaname}.d_order_lifecycle swap with &{schemaname}.d_order_lifecycle;
