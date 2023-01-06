-----------------------------------------------------------------------------
---------- Subscription ts				 ---------
-----------------------------------------------------------------------------

alter session set QUOTED_IDENTIFIERS_IGNORE_CASE = false;

use role &{rolename};
use database &{dbname};
use warehouse &{whname};
use schema &{schemaname};

create or replace view subscription_ts
as
WITH
future_date as (select last_day(dateadd(year,10,current_date()),year) as fd)
select  ss.created as subscription_create_date,
        ss.updated as subscription_update_date,
        ss.account_id,
        ss.subscription_id,
        trim('v3'||':'||ss.subscription_id) as subscription_sk,
        rss.recurly_subscription_id,
        ss.product,
        ss.package,
        rss.plan_code,
        ss.manual_invoice as manual_invoice_ind,
        rss.start_date as subscription_start_date,
        rss.unit_price_cents/rss.billing_frequency_months as monthly_subscription_service_fee,
        rss.active as subscription_active_ind, -- convert boolean to 0/1 to assist summarization
        case when rss.active = False then rss.updated else future_date.fd end as subscription_cancelled_date, 
        rss.quantity as license_quantity
from ZENALYTICS.AMS_ACCOUNTS.subscription ss,
     ZENALYTICS.AMS_ACCOUNTS.recurly_subscription rss,
     future_date
where ss.recurly_subscription_key = rss.recurly_subscription_key
UNION ALL
select  ss.created as subscription_create_date,
        ss.updated as subscription_update_date,
        ss.account_id,
        ss.subscription_id,
        trim('v3'||':'||ss.subscription_id) as subscription_sk,
        rss.recurly_subscription_id,
        ss.product,
        ss.package,
        rss.plan_code,
        ss.manual_invoice as manual_invoice_ind,
        rss.start_date as subscription_start_date,
        rss.unit_price_cents/rss.billing_frequency_months as monthly_subscription_service_fee,
        rss.active as subscription_active_ind, -- convert boolean to 0/1 to assist summarization
        case when rss.active = False then rss.updated else future_date.fd end as subscription_cancelled_date, 
        rss.quantity as license_quantity
from ZENALYTICS.AMS_ACCOUNTS.subscription_snapshot ss,
     ZENALYTICS.AMS_ACCOUNTS.recurly_subscription_snapshot rss,
     future_date
where ss.recurly_subscription_snapshot_id = rss.recurly_subscription_snapshot_id;



