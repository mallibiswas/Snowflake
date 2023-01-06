---------------------------------------------------------------
-------------------------- AMS SNAPSHOTS  ---------------------
---------------------------------------------------------------

alter session set QUOTED_IDENTIFIERS_IGNORE_CASE = false;

use database &{dbname};
use warehouse &{whname};
use schema &{schemaname};
use role &{rolename};

-- load ams snapshots
MERGE INTO &{dbname}.&{schemaname}.subscriptions_v2_snapshot as target USING
(
	select 
	$1::integer as subscription_id,
	$2::timestamp as created,
	$3::timestamp as updated,
	$4::integer as account_id,
	$5::integer as billing_account_id,
	$6::integer as plan_id,
	$7::integer as previous_subscription_id,
	$8::integer as contract_id,
	$9 as subscription_state,
	$10::integer as service_fee,
	$11::timestamp as subscription_start_date,
	$12::integer as trial_length,
	$13::integer as number_of_billing_cycles,
	$14::integer as wait_period,
	$15 as recurly_subscription_id,
	$16 as sales_rep,
	$17 as notes,
	$18 as salesforce_opportunity_id,
	$19 as salesforce_id,
	$20::integer as old_world_subscription_id,
	$21::integer as renewal_term_months,
	$22::integer as package_id,
	$23 as recurly_coupon_id,
	$24::date as absolute_trial_end_date,
	$25 as contract_expiry_pivot,
	$26 as payment_date_pivot,
	'&{asof_date}'::date as asof_date
	FROM @&{stagename}/&{stagepath}/subscriptions_v2.csv
) as source ON target.asof_date = source.asof_date
WHEN not matched then insert (SUBSCRIPTION_ID,
				CREATED,
				UPDATED,
				ACCOUNT_ID,
				BILLING_ACCOUNT_ID,
				PLAN_ID,
				PREVIOUS_SUBSCRIPTION_ID,
				CONTRACT_ID,
				SUBSCRIPTION_STATE,
				SERVICE_FEE,
				SUBSCRIPTION_START_DATE,
				TRIAL_LENGTH,
				NUMBER_OF_BILLING_CYCLES,
				WAIT_PERIOD,
				RECURLY_SUBSCRIPTION_ID,
				SALES_REP,
				NOTES,
				SALESFORCE_OPPORTUNITY_ID,
				SALESFORCE_ID,
				OLD_WORLD_SUBSCRIPTION_ID,
				RENEWAL_TERM_MONTHS,
				PACKAGE_ID,
				RECURLY_COUPON_ID,
				ABSOLUTE_TRIAL_END_DATE,
				CONTRACT_EXPIRY_PIVOT,
				PAYMENT_DATE_PIVOT,
				ASOF_DATE)
values (source.SUBSCRIPTION_ID,
	source.CREATED,
	source.UPDATED,
	source.ACCOUNT_ID,
	source.BILLING_ACCOUNT_ID,
	source.PLAN_ID,
	source.PREVIOUS_SUBSCRIPTION_ID,
	source.CONTRACT_ID,
	source.SUBSCRIPTION_STATE,
	source.SERVICE_FEE,
	source.SUBSCRIPTION_START_DATE,
	source.TRIAL_LENGTH,
	source.NUMBER_OF_BILLING_CYCLES,
	source.WAIT_PERIOD,
	source.RECURLY_SUBSCRIPTION_ID,
	source.SALES_REP,
	source.NOTES,
	source.SALESFORCE_OPPORTUNITY_ID,
	source.SALESFORCE_ID,
	source.OLD_WORLD_SUBSCRIPTION_ID,
	source.RENEWAL_TERM_MONTHS,
	source.PACKAGE_ID,
	source.RECURLY_COUPON_ID,
	source.ABSOLUTE_TRIAL_END_DATE,
	source.CONTRACT_EXPIRY_PIVOT,
	source.PAYMENT_DATE_PIVOT,
	source.ASOF_DATE)
;

--------------------------------------------

MERGE INTO &{dbname}.&{schemaname}.account_snapshot as target USING
(
	select 
	$1::integer as account_id,
	$2::timestamp as created,
	$3::timestamp as updated,
	$4 as name,
	$5 as salesforce_id,
	$6 as business_profile_id,
	$7 as account_state,
	$8::boolean as disqualified,
	$9 as billing_mode,
	$10::integer as partner_account_id,
	$11::integer as billing_account_id,
	$12::boolean as is_test,
	'&{asof_date}'::date as asof_date
	FROM @&{stagename}/&{stagepath}/account.csv
) as source ON target.asof_date = source.asof_date
WHEN not matched then insert (ACCOUNT_ID,
				CREATED,
				UPDATED,
				NAME,
				SALESFORCE_ID,
				BUSINESS_PROFILE_ID,
				ACCOUNT_STATE,
				DISQUALIFIED,
				BILLING_MODE,
				PARTNER_ACCOUNT_ID,
				BILLING_ACCOUNT_ID,
				IS_TEST,
				ASOF_DATE)
values (source.ACCOUNT_ID,
	source.CREATED,
	source.UPDATED,
	source.NAME,
	source.SALESFORCE_ID,
	source.BUSINESS_PROFILE_ID,
	source.ACCOUNT_STATE,
	source.DISQUALIFIED,
	source.BILLING_MODE,
	source.PARTNER_ACCOUNT_ID,
	source.BILLING_ACCOUNT_ID,
	source.IS_TEST,
	source.ASOF_DATE)
;

--------------------------------------------

MERGE INTO &{dbname}.&{schemaname}.subscriptions_v2_location_through_snapshot as target USING
(
	select 
	$1::integer as subscriptions_location_id,
	$2::integer as subscription_id,
	$3::integer as location_id,
	'&{asof_date}'::date as asof_date
	FROM @&{stagename}/&{stagepath}/subscriptions_v2_location_through.csv
) as source ON target.asof_date = source.asof_date
WHEN not matched then insert (SUBSCRIPTIONS_LOCATION_ID,
				SUBSCRIPTION_ID,
				LOCATION_ID,
				ASOF_DATE) 
values (source.SUBSCRIPTIONS_LOCATION_ID,
	source.SUBSCRIPTION_ID,
	source.LOCATION_ID,
	source.ASOF_DATE)
;


