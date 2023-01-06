----------------------------------------------------------
------ SFDC Churn Requests 
----------------------------------------------------------

alter session set QUOTED_IDENTIFIERS_IGNORE_CASE = false;
alter session set TIMEZONE = 'UTC';

use role &{rolename};
use database &{dbname};
use warehouse &{whname};
use schema &{schemaname};

create or replace table &{dbname}.&{stageschemaname}.d_sfdc_churn_requests
as
WITH churn_requests as 
        (SELECT
             Account__c as salesforce_account_id, 
             CHURN_SUMMARY__C as churn_summary,
             CHURN_STATUS__C as churn_status,
             CONCAT(
                IFF(CHURN_REASON_BUSINESS_SITUATION__C is null,'',CONCAT(CHURN_REASON_BUSINESS_SITUATION__C,';',CHR(13),CHR(10))),
                IFF(CHURN_REASON_CONTRACT_BILLING__C is null,'',CONCAT(CHURN_REASON_CONTRACT_BILLING__C,';',CHR(13),CHR(10))),
                IFF(CHURN_REASON_DETAILS__C is Null,'',CONCAT(CHURN_REASON_DETAILS__C,';',CHR(13),CHR(10))),
                IFF(CHURN_REASON_PRODUCT__C is Null,'',CONCAT(CHURN_REASON_PRODUCT__C,';',CHR(13),CHR(10))),
                IFF(CHURN_REASON_SERVICE__C is Null,'',CONCAT(CHURN_REASON_SERVICE__C,';',CHR(13),CHR(10))),
                IFF(CHURN_REASON_TECHNICAL__C is Null,'',CONCAT(CHURN_REASON_TECHNICAL__C,';',CHR(13),CHR(10)))           
             ) as Churn_Reason, 
        row_number() over (partition by Account__c order by CreatedDate DESC, CHURN_SUMMARY__C desc) as rn
        FROM SFDC.CHURN_REQUEST__C 
        )
select  o.account_id, 
        a.subscription_id, 
        subs.product,
        cr.salesforce_account_id, 
        sacct.name, 
        o.order_id,
        a.asset_id, 
        to_date(left(so.type, 10)) as churn_requested_date, 
        not(rsubs.active) as cancelled, -- cancelled subs
        cr.churn_status, 
        cr.churn_summary, 
        cr.Churn_Reason,
        rsubs.recurly_subscription_id,
        rsubs.updated as cancelled_date, 
        rsubs.quantity, 
        rsubs.unit_price_cents, 
        rsubs.billing_frequency_months,
	current_date as asof_date
from ams_accounts."ORDER" o, 
   ams_accounts.order_item oi,
   ams_accounts.asset a,                   
   ams_accounts.salesforce_order so, 
  ams_accounts.subscription subs, 
  ams_accounts.recurly_subscription rsubs,
  ams_accounts.account acct,
  ams_accounts.salesforce_account sacct,
  churn_requests cr
where 
 subs.recurly_subscription_key = rsubs.recurly_subscription_key
and a.subscription_id = subs.subscription_id
and o.account_id = acct.account_id
and acct.salesforce_account_id = cr.salesforce_account_id
and sacct.salesforce_account_id = cr.salesforce_account_id
and o.salesforce_order_id = so.salesforce_order_id
-- and to_date(left(so.type, 10)) = to_date(rsubs.updated) -- churn request date = cancelled date
and rsubs.active = False
and o.order_id = oi.order_id
and a.asset_id = oi.asset_id
and o.cancelled is null and oi.salesforce_asset_synced = True and oi.dirty = False
and to_date(a.created) <> to_date(a.updated)
and a.item_type='subscription'
and cr.rn=1 
order by churn_requested_date desc;

alter table &{stageschemaname}.d_sfdc_churn_requests  swap with &{schemaname}.d_sfdc_churn_requests;

