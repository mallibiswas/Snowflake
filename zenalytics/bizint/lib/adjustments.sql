-------------------------------------------------------------------
---------- d_adjustments: Create paused subscriptions table 
-------------------------------------------------------------------

alter session set QUOTED_IDENTIFIERS_IGNORE_CASE = false;
alter session set TIMEZONE = 'UTC';

use role &{rolename};
use database &{dbname};
use warehouse &{whname};
use schema &{stageschemaname};


--------------------------------------------------------------------------------------------------------------
--------- Create paused subscriptions table / Mar 28th 2020
--------------------------------------------------------------------------------------------------------------


/***
way to solve this problem
1. create anchor key on (account_id,subscription_create_date,product,rownum)
2. create a master list of all the anchor keys by select key from before union select key from after
3. left join before and after to the master list
*/


create or replace temporary table _before_ 
as
-- BEFORE / Subscription level
-- find subs that where cancelled with corresponding subs created at the same time
WITH active_accts as (
        select distinct account_id, product, plan_code, subscription_cancelled_date, subscription_create_date
        from bizint.subscription_ts b
        where b.subscription_active_ind = True
        )
     select bef.account_id, 
            subscription_create_date, 
            subscription_start_date, 
            bef.subscription_id, 
            bef.recurly_subscription_id, 
            product,
            plan_code,
            monthly_subscription_service_fee, 
            license_quantity,
            subscription_cancelled_date,
            row_number() over (partition by bef.account_id, subscription_cancelled_date order by subscription_start_date) as seqnum
     from zenalytics.bizint.subscription_ts bef
     where exists (select 'x' from active_accts 
                   where active_accts.account_id = bef.account_id 
                   and active_accts.subscription_create_date = bef.subscription_cancelled_date
                   and product=bef.product 
                   and plan_code = bef.plan_code)
     and subscription_active_ind = False
;


create or replace temporary table _after_ 
as
-- AFTER
-- find subs that where created with corresponding subs cancelled at the same time
WITH cancelled_accts as (
        select distinct account_id, product, plan_code, subscription_cancelled_date, subscription_create_date 
        from bizint.subscription_ts 
        where subscription_active_ind = False
        )
select account_id, 
      subscription_create_date,        
      subscription_start_date, 
      subscription_id, 
      recurly_subscription_id, 
      product,
      plan_code,
      monthly_subscription_service_fee, 
      license_quantity,
      subscription_cancelled_date, 
      row_number() over (partition by account_id,subscription_create_date order by subscription_start_date) as seqnum
from zenalytics.bizint.subscription_ts aft
where exists (select 'x' from cancelled_accts 
              where cancelled_accts.account_id = aft.account_id 
              and cancelled_accts.subscription_cancelled_date = aft.subscription_create_date
              and product = aft.product 
              and plan_code = aft.plan_code)
;


create or replace temporary table _keys_
as 
select account_id,
      product,
      plan_code,
      subscription_cancelled_date as event_datetime,
      seqnum
      from _before_
UNION
select account_id,
      product,
      plan_code,
      subscription_create_date as event_datetime,
      seqnum
      from _after_
;

create or replace table  &{stageschemaname}.d_adjustments
as
select  k.account_id, 
        k.event_datetime,   
        k.product,
        k.plan_code,
        k.seqnum,
        a.salesforce_account_id, 
        bef.subscription_id, 
        bef.recurly_subscription_id, 
        bef.subscription_start_date::date as subscription_start_date, 
        bef.monthly_subscription_service_fee/100 as mrr,
        bef.license_quantity as quantity, 
        bef.subscription_cancelled_date,  
        aft.subscription_id as subscription_id_after_pause, 
        aft.subscription_create_date,
        aft.license_quantity as quantity_after_pause, 
        ADD_MONTHS(aft.subscription_start_date,-1*floor(datediff(month,aft.subscription_create_date,aft.subscription_start_date)))::date as paused_date,
        datediff(day,paused_date,aft.subscription_start_date) as days_paused, -- diff btwn paused date and next start date
        aft.subscription_start_date::date as start_date_after_pause, 
        aft.recurly_subscription_id as recurly_subscription_id_after_pause,
        aft.monthly_subscription_service_fee/100 as mrr_after_pause,
        aft.subscription_cancelled_date::date as cancelled_date_after_pause,
        current_date as asof_date
from _keys_ k
inner join zenalytics.ams_accounts.account a on k.account_id = a.account_id 
left join _before_ bef  ON k.account_id = bef.account_id 
                        and k.event_datetime = bef.subscription_cancelled_date
                        and k.product = bef.product
                        and k.plan_code = bef.plan_code
                        and k.seqnum = bef.seqnum 
left join _after_ aft   ON k.account_id = aft.account_id 
                        and k.event_datetime = aft.subscription_create_date
                        and k.product = aft.product
                        and k.plan_code = aft.plan_code
                        and k.seqnum = aft.seqnum
;


alter table &{stageschemaname}.d_adjustments  swap with &{schemaname}.d_adjustments;
