----------------------------------------------------------
------ Delinquency Life Cycle
----------------------------------------------------------

alter session set QUOTED_IDENTIFIERS_IGNORE_CASE = false;
alter session set TIMEZONE = 'UTC';

use role &{rolename};
use database &{dbname};
use warehouse &{whname};
use schema &{stageschemaname};

SET ASOF_DATE = (SELECT Current_Date());

create or replace table d_delinquency_lifecycle
as
WITH
future_date as (select last_day(dateadd(year,10,current_date()),year) as fd),
subscriptions as (select account_id, subscription_id, recurly_subscription_id from &{sourcedb}.RECURLY.recurly_subscriptions),
allinvoices as (
  select ri.account_id,
         ri.subscription_id,
         recurly_subscription_id,
         nvl(ri.collection_method,'automatic') as collection_method,
         ri.state,
         ri.created_at,
         dateadd('day',nvl(ri.net_terms,0), ri.created_at) as due_date,
         invoice_number
    from &{sourcedb}.RECURLY.recurly_invoices ri, subscriptions rs
    where ri.subscription_id = rs.subscription_id and ri.account_id = rs.account_id
    and ri.net_terms is not null
--    and total_in_cents > 0 -- account for Freemium subscriptions
),
reflist as (
  select account_id,
         subscription_id,
         recurly_subscription_id,
         collection_method,
         count(invoice_number) over (partition by subscription_id, collection_method) as number_of_invoices,
         state,
         due_date,
         FIRST_VALUE(created_at) over (partition by subscription_id, collection_method order by created_at) as first_invoiced_at,
         case when state in ('paid') then FIRST_VALUE(created_at) over (partition by subscription_id, collection_method, state order by created_at) end as first_paid_at,
         case when state in ('paid') then LAST_VALUE(created_at) over (partition by subscription_id, collection_method, state order by created_at) end as last_paid_at,
         case when state in ('failed','past_due') then FIRST_VALUE(created_at) over (partition by subscription_id, collection_method, state order by created_at) end as first_unpaid_at,
         case when state in ('failed','past_due') then LAST_VALUE(created_at) over (partition by subscription_id, collection_method, state order by created_at) end as last_unpaid_at,
         case when state <> lag(state) over (partition by subscription_id, collection_method order by created_at) then created_at
              when invoice_number = FIRST_VALUE(invoice_number) over (partition by subscription_id, collection_method order by created_at) then created_at
              when invoice_number = LAST_VALUE(invoice_number) over (partition by subscription_id, collection_method order by created_at) then created_at
         else null end as ref_created_at,
         case when state <> lag(state) over (partition by subscription_id, collection_method order by created_at) then invoice_number
              when invoice_number = FIRST_VALUE(invoice_number) over (partition by subscription_id, collection_method order by created_at) then invoice_number
              when invoice_number = LAST_VALUE(invoice_number) over (partition by subscription_id, collection_method order by created_at) then invoice_number
         else null end as ref_invoice_number
    from allinvoices
),
state_boundaries as (
  select account_id,
        subscription_id,
        recurly_subscription_id,
        collection_method,
        state,
        number_of_invoices,
        due_date,
        first_invoiced_at,
        first_paid_at,
        last_paid_at,
        first_unpaid_at,
        last_unpaid_at,
        b.ref_created_at as state_begin_at,
        lead(ref_created_at) over (partition by subscription_id, collection_method order by ref_created_at) as state_end_at,
        datediff(day,b.ref_created_at,lead(ref_created_at) over (partition by subscription_id, collection_method order by ref_created_at)) as days_in_state, -- = days betwn state begin and state end
        b.ref_invoice_number as begin_invoice_number,
        lead(ref_invoice_number) over (partition by subscription_id, collection_method order by ref_invoice_number) as end_invoice_number
from reflist b
where ref_invoice_number is not null
)
select  s.account_id,
        a.company_name as account_name,
        subscription_id,
        recurly_subscription_id,
        collection_method,
        s.state,
        due_date,
        number_of_invoices,
        state_begin_at as state_begin_date,
        state_end_at as state_end_date,
        first_invoiced_at as first_invoiced_date,
        first_paid_at as invoice_first_paid_date,
        last_paid_at as invoice_last_paid_date,
        first_unpaid_at as invoice_first_unpaid_date,
        last_unpaid_at as invoice_last_unpaid_date,
        case when collection_method = 'automatic' then first_paid_at else first_invoiced_at end as subscription_realized_date,
        case when collection_method = 'automatic' then MIN(first_paid_at) over (partition by subscription_id order by due_date asc)
                    else MIN(first_invoiced_at) over (partition by subscription_id order by due_date asc) end as subscription_first_realized_date,
        days_in_state,
        begin_invoice_number,
        end_invoice_number,
        case when s.state in ('failed','past_due') and days_in_state > 120 then dateadd(day,120, state_begin_at) else future_date.fd end as delinquency_begin_date,
        case when s.state in ('failed','past_due') and days_in_state > 120 then state_end_at else future_date.fd end as delinquency_end_date,
        rank() over (partition by subscription_id order by state_begin_at asc) as delinquency_lifecycle_number
from    state_boundaries s,
        &{sourcedb}.RECURLY.recurly_accounts a,
        future_date
where s.account_id = a.account_id
and (case when number_of_invoices = 1 then 1 else end_invoice_number end) is not null;


alter table d_delinquency_lifecycle add column asof_date date;
update d_delinquency_lifecycle set asof_date = $ASOF_DATE;

alter table &{stageschemaname}.d_delinquency_lifecycle swap with &{schemaname}.d_delinquency_lifecycle;
