-----------------------------------------------------------------------
------------------------------- CONTRACT ------------------------------
-----------------------------------------------------------------------

use warehouse &{whname};

truncate table &{dbname}._STAGING.CONTRACT;

copy into  &{dbname}._STAGING.CONTRACT
from
(
select 
	$1::integer as contract_id,
	$2::timestamp as created,
	$3::timestamp as updated,
	$4::integer as account_id,
	$5::integer as subscription_id,
	$6::integer as billing_account_id,
	$7 as salesforce_id,
	$8::integer as service_fee,
	$9::integer as hardware_fee,
	$10 as num_aps,
	$11 as payment_term,
	$12 as payment_method,
	$13 as signer_name,
	$14 as signer_email,
	$15::integer as pilot_length,
	$16 as pilot_type,
	$17::integer as pilot_extension,
	$18::date as effective_date,
	$19::date as expiration_date,
	$20 as renewal_term,
	$21::timestamp as sent,
	$22::timestamp as signed,
	$23 as contract_state,
	$24::boolean as beta,
	$25 as sales_rep,
	$26::integer as wait_period,
	$27::integer as category_id,
	$28 as business_unit,
	$29::boolean as use_ams_contract,
	$30::boolean as use_ams_billing,
	$31 as agreement_version,
	$32 as salesforce_opportunity_id,
	'&{asof_date}'::date as asof_date
    from @&{stagename}/&{stagepath}/contract.csv
);

alter table &{dbname}._STAGING.CONTRACT swap with &{dbname}.AMS.CONTRACT;
