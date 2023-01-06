-------------------------------------------------------
--------- SUBSCRIPTIONS_V2_LOCATION_THROUGH -----------
-------------------------------------------------------

use warehouse &{whname};

truncate table &{dbname}._STAGING.SUBSCRIPTIONS_V2_LOCATION_THROUGH;

copy into  &{dbname}._STAGING.SUBSCRIPTIONS_V2_LOCATION_THROUGH
from
(
	select 
	$1::integer as subscriptions_location_id,
	$2::integer as subscription_id,
	$3::integer as location_id,
	'&{asof_date}'::date as asof_date
    from @&{stagename}/&{stagepath}/subscriptions_v2_location_through.csv
);

alter table &{dbname}._STAGING.SUBSCRIPTIONS_V2_LOCATION_THROUGH swap with &{dbname}.AMS.SUBSCRIPTIONS_V2_LOCATION_THROUGH;
