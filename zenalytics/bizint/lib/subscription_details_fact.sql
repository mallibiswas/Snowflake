-------------------------------------------------------------------
---------- Subscription details / AMS V3 
-------------------------------------------------------------------

alter session set QUOTED_IDENTIFIERS_IGNORE_CASE = false;
alter session set TIMEZONE = 'UTC';

use role &{rolename};
use database &{dbname};
use warehouse &{whname};
use schema &{stageschemaname};

SET ASOF_DATE = (SELECT Current_Date());

SET LAST_REPORT_DATE = (SELECT max(report_date) from &{schemaname}.subscription_details_fact);
SET LOOKBACK7_DATE = (SELECT dateadd(day,-14,to_date($LAST_REPORT_DATE)));
SET LOOKBACK3_DATE = (SELECT dateadd(day,-7,to_date($LAST_REPORT_DATE)));


create or replace temporary table &{stageschemaname}._subscription_details_fact
as
WITH date_dim as (select report_date, report_date_sk
                  from &{schemaname}.d_date
                  where report_date >= to_date('2015-01-01') and report_date <= current_date()),
future_date as (select last_day(dateadd(year,10,current_date()),year) as fd)
select date_dim.report_date,
       account_id,
       subscription_id,
       subscription_migrated_date,
       Product,
       case when (date_dim.report_date between to_date(subscription_first_realized_date) and to_date(churn_begin_date)) then true else false end as active_subscription_fl,
       case when date_dim.report_date = to_date(subscription_first_realized_date) then true else False end as new_realized_fl,
       case when date_dim.report_date = to_date(subscription_cancelled_date) then True else False end::boolean as cancelled_fl,
       case when date_dim.report_date = to_date(subscription_upgrade_date) then True else False end::boolean as upgrade_fl,
       case when date_dim.report_date = to_date(subscription_downgrade_date) then True else False end::boolean as downgrade_fl,
       case when date_dim.report_date = to_date(churn_begin_date) then True else False end::boolean as churn_fl,
       case when report_date = to_date(subscription_upgrade_date) then True else False end::boolean as subscription_expansion_fl,
       case when report_date = to_date(delinquency_begin_date) then True else False end::boolean as delinquency_churn_fl,
       case when report_date = to_date(subscription_churn_date) then True else False end::boolean as subscription_churn_fl,
       case when report_date = to_date(MNIR_date) then True else False end::boolean as MNIR_fl, -- added reactivation flag 11/7
       case when report_date = to_date(subscription_adjustment_date) then True else False end::boolean as subscription_adjustment_fl,
       monthly_subscription_service_fee,
       license_quantity
from 	&{schemaname}.d_subscription_lifecycle l, date_dim, future_date
where date_dim.report_date >= to_date(subscription_first_realized_date) -- realized subs only
and date_dim.report_date <= nvl(to_date(churn_begin_date),report_date)
and nvl(to_date(churn_begin_date),future_date.fd) <= future_date.fd
and date_dim.report_date >= to_date(state_begin_date)  -- bound each subscription values to within it's valid dates
and date_dim.report_date < to_date(state_end_date)
and date_dim.report_date >= to_date(subscription_migrated_date) -- since migration or new subs only
;


create or replace table &{stageschemaname}.subscription_details_fact
as
WITH date_dim as (select report_date, report_date_sk
                  from &{schemaname}.d_date
                  where report_date >= to_date('2015-01-01') and report_date <= current_date()),
future_date as (select last_day(dateadd(year,10,current_date()),year) as fd)
select REPORT_DATE,
        ACCOUNT_ID,
        SUBSCRIPTION_ID,
        SUBSCRIPTION_MIGRATED_DATE,
        PRODUCT,
        ACTIVE_SUBSCRIPTION_FL,
        NEW_REALIZED_FL,
        CANCELLED_FL,
        UPGRADE_FL,
        DOWNGRADE_FL,
        CHURN_FL,
        SUBSCRIPTION_EXPANSION_FL,
        DELINQUENCY_CHURN_FL,
        SUBSCRIPTION_CHURN_FL,
        MNIR_FL,
	SUBSCRIPTION_ADJUSTMENT_FL,
        MONTHLY_SUBSCRIPTION_SERVICE_FEE,
        MONTHLY_SUBSCRIPTION_SERVICE_FEE - NVL(LAG(MONTHLY_SUBSCRIPTION_SERVICE_FEE) over (partition by subscription_id order by report_date),0) as mrr_expansion_contraction,
        LICENSE_QUANTITY,
        LICENSE_QUANTITY - NVL(LAG(LICENSE_QUANTITY) over (partition by subscription_id order by report_date),0) as license_expansion_contraction
from    &{stageschemaname}._subscription_details_fact
where  report_date >= $LOOKBACK7_DATE
;

alter table &{stageschemaname}.subscription_details_fact add column asof_date date;
update &{stageschemaname}.subscription_details_fact set asof_date = $ASOF_DATE;

MERGE into &{schemaname}.subscription_details_fact as target using (select * from &{stageschemaname}.subscription_details_fact where report_date >= $LOOKBACK3_DATE) as source 
on target.account_id = source.account_id 
and target.subscription_id = source.subscription_id 
and target.report_date = source.report_date
WHEN matched then update set    target.REPORT_DATE=source.REPORT_DATE,
				target.ACCOUNT_ID=source.ACCOUNT_ID,
				target.SUBSCRIPTION_ID=source.SUBSCRIPTION_ID,
				target.SUBSCRIPTION_MIGRATED_DATE=source.SUBSCRIPTION_MIGRATED_DATE,
				target.PRODUCT=source.PRODUCT,
				target.ACTIVE_SUBSCRIPTION_FL=source.ACTIVE_SUBSCRIPTION_FL,
				target.NEW_REALIZED_FL=source.NEW_REALIZED_FL,
				target.CANCELLED_FL=source.CANCELLED_FL,
				target.UPGRADE_FL=source.UPGRADE_FL,
				target.DOWNGRADE_FL=source.DOWNGRADE_FL,
				target.CHURN_FL=source.CHURN_FL,
				target.SUBSCRIPTION_EXPANSION_FL=source.SUBSCRIPTION_EXPANSION_FL,
				target.DELINQUENCY_CHURN_FL=source.DELINQUENCY_CHURN_FL,
				target.SUBSCRIPTION_CHURN_FL=source.SUBSCRIPTION_CHURN_FL,
				target.MNIR_FL=source.MNIR_FL,
				target.SUBSCRIPTION_ADJUSTMENT_FL=source.SUBSCRIPTION_ADJUSTMENT_FL,
				target.MONTHLY_SUBSCRIPTION_SERVICE_FEE=source.MONTHLY_SUBSCRIPTION_SERVICE_FEE,
				target.MRR_EXPANSION_CONTRACTION=source.MRR_EXPANSION_CONTRACTION,
				target.LICENSE_QUANTITY=source.LICENSE_QUANTITY,
				target.LICENSE_EXPANSION_CONTRACTION=source.LICENSE_EXPANSION_CONTRACTION,
				target.ASOF_DATE=source.ASOF_DATE

WHEN not MATCHED then insert (	REPORT_DATE,
				ACCOUNT_ID,
				SUBSCRIPTION_ID,
				SUBSCRIPTION_MIGRATED_DATE,
				PRODUCT,
				ACTIVE_SUBSCRIPTION_FL,
				NEW_REALIZED_FL,
				CANCELLED_FL,
				UPGRADE_FL,
				DOWNGRADE_FL,
				CHURN_FL,
				SUBSCRIPTION_EXPANSION_FL,
				DELINQUENCY_CHURN_FL,
				SUBSCRIPTION_CHURN_FL,
				MNIR_FL,
				SUBSCRIPTION_ADJUSTMENT_FL,
				MONTHLY_SUBSCRIPTION_SERVICE_FEE,
				MRR_EXPANSION_CONTRACTION,
				LICENSE_QUANTITY,
				LICENSE_EXPANSION_CONTRACTION,
				ASOF_DATE)
values ( 			source.REPORT_DATE,
				source.ACCOUNT_ID,
				source.SUBSCRIPTION_ID,
				source.SUBSCRIPTION_MIGRATED_DATE,
				source.PRODUCT,
				source.ACTIVE_SUBSCRIPTION_FL,
				source.NEW_REALIZED_FL,
				source.CANCELLED_FL,
				source.UPGRADE_FL,
				source.DOWNGRADE_FL,
				source.CHURN_FL,
				source.SUBSCRIPTION_EXPANSION_FL,
				source.DELINQUENCY_CHURN_FL,
				source.SUBSCRIPTION_CHURN_FL,
				source.MNIR_FL,
				source.SUBSCRIPTION_ADJUSTMENT_FL,
				source.MONTHLY_SUBSCRIPTION_SERVICE_FEE,
				source.MRR_EXPANSION_CONTRACTION,
				source.LICENSE_QUANTITY,
				source.LICENSE_EXPANSION_CONTRACTION,
				source.ASOF_DATE);


