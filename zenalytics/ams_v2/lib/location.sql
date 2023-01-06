----------------------------------------------------------
------------------------- LOCATION ------------------------
----------------------------------------------------------

use warehouse &{whname};

truncate table &{dbname}._STAGING.LOCATION;

copy into  &{dbname}._STAGING.LOCATION
from
(
	select 
	$1::integer as location_id,
	$2::timestamp as created,
	$3::timestamp as updated,
	$4::integer as account_id,
	$5 as name,
	$6 as salesforce_id,
	$7 as location_state,
	$8 as address_line1,
	$9 as address_line2,
	$10 as address_zip,
	$11 as address_city,
	$12 as address_state,
	$13 as address_country,
	$14 as business_profile_id,
	$15::float as latitude,
	$16::float as longitude,
	$17 as installed,
	$18::boolean as disqualified,
	$19::integer as billing_account_id,
	$20::integer as billing_account_v2_id,
	'&{asof_date}'::date as asof_date
    from @&{stagename}/&{stagepath}/location.csv
);

alter table &{dbname}._STAGING.LOCATION swap with &{dbname}.AMS.LOCATION;
