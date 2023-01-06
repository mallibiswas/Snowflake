-------------------------------------------------------------------
---------- Offsetting Subscriptions to identify adjustments  
-------------------------------------------------------------------

alter session set QUOTED_IDENTIFIERS_IGNORE_CASE = false;
alter session set TIMEZONE = 'UTC';

use role &{rolename};
use database &{dbname};
use warehouse &{whname};
use schema &{stageschemaname};

create or replace table &{stageschemaname}.d_offsetting_subscriptions
as  
select  a.account_id, 
        a.product, 
        a.subscription_id, 
        a.subscription_cancelled_date, 
        a.license_quantity, 
        b.subscription_id as offsetting_subscription_id, 
        b.license_quantity as offsetting_license_quantity, 
        b.subscription_create_date as offsetting_subscription_create_date,
        current_date as asof_date
from &{schemaname}.subscription_ts a, 
     &{schemaname}.subscription_ts b 
Where a.account_id = b.account_id
and date_trunc(day,b.subscription_create_date) between dateadd(day,-7,date_trunc(day,a.subscription_cancelled_date))  and dateadd(day,7,date_trunc(day,a.subscription_cancelled_date)) 
-- new subs created within 7 days of old cancellation [May not be realized]
and a.product = b.product
and b.subscription_active_ind = True
and a.subscription_active_ind = False
-- ignore offsets when subs created and cancelled same day 
and date_trunc(day,b.subscription_create_date) <> date_trunc(day,b.subscription_cancelled_date); 


alter table &{stageschemaname}.d_offsetting_subscriptions swap with &{schemaname}.d_offsetting_subscriptions;



