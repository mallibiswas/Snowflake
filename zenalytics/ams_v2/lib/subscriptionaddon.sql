-------------------------------------------------------
------------------ SUBSCRIPTIONADDON ------------------
-------------------------------------------------------

use warehouse &{whname};

truncate table &{dbname}._STAGING.SUBSCRIPTIONADDON;

copy into  &{dbname}._STAGING.SUBSCRIPTIONADDON
from
(
	select 
	$1::integer as subscriptionaddon_id,
	$2::timestamp as created,
	$3::timestamp as updated,
	$4::integer as subscription_id,
	$5::integer as plan_addon_id,
	$6::integer as subscription_addon_price_id,
	$7::integer as qty,
	'&{asof_date}'::date as asof_date
    from @&{stagename}/&{stagepath}/subscriptionaddon.csv
);

alter table &{dbname}._STAGING.SUBSCRIPTIONADDON swap with &{dbname}.AMS.SUBSCRIPTIONADDON;
