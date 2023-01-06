----------------------------------------------------------
-- Customer (Account) Lifecycle
----------------------------------------------------------

alter session set QUOTED_IDENTIFIERS_IGNORE_CASE = false;
alter session set TIMEZONE = 'UTC';

use role &{rolename};
use database &{dbname};
use warehouse &{whname};
use schema &{stageschemaname};

SET ASOF_DATE = (SELECT Current_Date());

create or replace table d_customer_lifecycle
as
WITH account_states as (
select account_id, 
  case when parse_json(payload):new_state::string = 'NEW' then event_time else null end as account_created_date, 
  case when parse_json(payload):new_state::string = 'ACTIVE' then event_time else null end as account_activated_date, 
  case when parse_json(payload):new_state::string = 'CLOSED' then event_time else null end as account_terminated_date 
from &{sourcedb}.AMS.event 
where entity_name = 'account'
and event_name = 'STATE'
),
order_states as (select  account_id, 
        min(contract_sent_date) as first_contract_sent_date,
        min(contract_signed_date) as first_contract_signed_date,
        min(subscription_start_date) as first_subscription_start_date,
        max(contract_cancelled_date) as last_contract_cancelled_date
from d_order_lifecycle 
group by account_id),
account_lc as (
select account_id, 
  max(account_created_date) as account_created_date,
  max(account_activated_date) as account_activated_date,
  max(account_terminated_date) as account_terminated_date
from account_states
group by account_id
)
select a.account_id,
  name as account_name,
  salesforce_id,
  partner_account_id,
  billing_account_id,
  account_created_date,
  account_activated_date,
  account_terminated_date,
  first_contract_sent_date,
  first_contract_signed_date,
  first_subscription_start_date,
  last_contract_cancelled_date
from &{sourcedb}.AMS.account a 
inner join account_lc on a.account_id = account_lc.account_id 
inner join order_states on a.account_id = order_states.account_id
where is_test = False and disqualified = False
;  

alter table d_customer_lifecycle add column asof_date date;
update d_customer_lifecycle set asof_date = $ASOF_DATE;

alter table &{stageschemaname}.d_customer_lifecycle swap with &{schemaname}.d_customer_lifecycle;
