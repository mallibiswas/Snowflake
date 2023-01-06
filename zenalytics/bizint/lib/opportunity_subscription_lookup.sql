----------------------------------------------------------
------ Opportunity (Id) to Recurly (Id) Lookup/Bridge table 
----------------------------------------------------------

alter session set QUOTED_IDENTIFIERS_IGNORE_CASE = false;
alter session set TIMEZONE = 'UTC';

use role &{rolename};
use database &{dbname};
use warehouse &{whname};
use schema &{schemaname};

create or replace temporary table _opportunity_subscription_lookup_ 
as
WITH ams_v3_subscriptions AS (

                      SELECT account_id, subscription_id, recurly_subscription_id  
                      FROM AMS_ACCOUNTS.SUBSCRIPTION s, 
			   AMS_ACCOUNTS.RECURLY_SUBSCRIPTION rs 
                      WHERE s.recurly_subscription_key = rs.recurly_subscription_key
),

ams_v3_opportunities as (  

          select 
            o.account_id,
            a.SUBSCRIPTION_ID,
            so.OPPORTUNITYID as SALESFORCE_OPPORTUNITY_ID 
          from  AMS_ACCOUNTS."ORDER" o, 
		SFDC."ORDER" so, 
		AMS_ACCOUNTS.ORDER_ITEM oi, 
		AMS_ACCOUNTS.ASSET a 
          where so.ID = o.SALESFORCE_ORDER_ID
          and o.ORDER_ID=oi.ORDER_ID
          and a.ASSET_ID=oi.ASSET_ID
          and item_type = 'subscription'

),

ams_v2_opportunities as (select to_varchar(account_id) as account_id, salesforce_opportunity_id, recurly_subscription_id from AMS.subscriptions_v2),

ams_v2_migrations as (select to_varchar(account_id) as account_id, salesforce_opportunity_id, recurly_subscription_id from ams.subscriptions_v2_pre_migration),

ams_v1_opportunities as (select to_varchar(s.account_id) as account_id, salesforce_opportunity_id, rs.recurly_subscription_id 
                         from AMS.contract c, AMS.subscription s, RECURLY.recurly_subscriptions rs
                         where c.subscription_id = s.subscription_id
                         and s.recurly_subscription_token = rs.recurly_subscription_id)

select 	'v1' as version, 
	account_id , 
	substr(salesforce_opportunity_id,1,15) as salesforce_opportunity_id, 
	recurly_subscription_id 
	from ams_v1_opportunities
UNION
select 	'v2' as version, 
	account_id, 
	substr(salesforce_opportunity_id,1,15) as salesforce_opportunity_id, 
	recurly_subscription_id 
	from ams_v2_opportunities
UNION
select 	'v2' as version, 
	account_id, 
	substr(salesforce_opportunity_id,1,15) as salesforce_opportunity_id, 
	recurly_subscription_id 
	from ams_v2_migrations
UNION
select 	'v3' as version, 
	v3s.account_id, 
	substr(salesforce_opportunity_id,1,15) as salesforce_opportunity_id, 
	recurly_subscription_id 
	from 	ams_v3_opportunities v3o, 
		ams_v3_subscriptions v3s 
	where 	v3o.subscription_id = v3s.subscription_id

;

create or replace table &{dbname}.&{schemaname}.opportunity_subscription_lookup 
as 
select 	o.*, 
	so.type as opportunity_type,
	current_date as asof_date 
from _opportunity_subscription_lookup_ o
LEFT JOIN &{dbname}.SFDC.opportunity so
ON o.salesforce_opportunity_id = SUBSTR(so.id,1,15);


