-----------------------------------------------------------------------
--------------- AMDASHBOARD_LOCATIONS_WITH_CONTRACTS ------------------
-----------------------------------------------------------------------

use warehouse &{whname};

truncate table &{dbname}._STAGING.AMDASHBOARD_LOCATIONS_WITH_CONTRACTS;

copy into &{dbname}._STAGING.AMDASHBOARD_LOCATIONS_WITH_CONTRACTS
from
(
	SELECT 
	$1 as business_id,
	$2::integer as account_id,
	$3 as account_name,
	$4::date as effective_date,
	$5::date as expiration_date,
	$6::integer as hardware_fee,
	$7 as payment_method,
	$8 as payment_term,
	$9::integer as pilot_extension,
	$10::integer as pilot_length,
	$11 as renewal_term,
	$12 as sales_rep,
	$13::integer as service_fee,
	$14::timestamp as signed,
	$15 as signer_email,
	$16::integer as contract_id,
	$17::integer as location_id,
	$18 as location_state,
	$19 as name,
	$20 as salesforce_id,
	$21::float as latitude,
	$22::float as longitude,
	$23 as ams_version,
	'&{asof_date}'::date as asof_date
	FROM @&{stagename}/&{stagepath}/amdashboard_locations_with_contracts.csv
);


alter table &{dbname}._STAGING.AMDASHBOARD_LOCATIONS_WITH_CONTRACTS swap with &{dbname}.AMS.AMDASHBOARD_LOCATIONS_WITH_CONTRACTS;
