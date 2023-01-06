-------------------------------------------------------
---------------------- SUBSCRIPTIONS_V2 ---------------
-------------------------------------------------------

use warehouse &{whname};

truncate table &{dbname}._STAGING.SUBSCRIPTIONS_V2;

copy into  &{dbname}._STAGING.SUBSCRIPTIONS_V2
from
(
	select 
	$1::integer as subscriptions_id,
	$2::timestamp as created,
	$3::timestamp as updated,
	$4::integer as account_id,
	$5::integer as billing_account_id,
	$6::integer as plan_id,
	$7::integer as previous_subscription_id,
	$8::integer as contract_id,
	$9 as subscription_state,
	$10::integer as service_fee,
	$11::date as subscription_start_date,
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
    from @&{stagename}/&{stagepath}/subscriptions_v2.csv
);

alter table &{dbname}._STAGING.SUBSCRIPTIONS_V2 swap with &{dbname}.AMS.SUBSCRIPTIONS_V2;
