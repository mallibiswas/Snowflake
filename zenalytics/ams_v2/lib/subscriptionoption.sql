-------------------------------------------------------
------------------ SUBSCRIPTIONOPTION ------------------
-------------------------------------------------------

use warehouse &{whname};

truncate table &{dbname}._STAGING.SUBSCRIPTIONOPTION;

copy into  &{dbname}._STAGING.SUBSCRIPTIONOPTION
from
(
	select 
	$1::integer as subscriptionoption_id,
	$2::timestamp as created,
	$3::timestamp as updated,
	$4 as target_payment_term,
	$5 as target_renewal_term,
	$6 as source_payment_term,
	$7 as source_renewal_term,
	$8::integer as fee_discount,
	$9 as group_code,
	$10::integer as target_plan_id,
	$11::integer as source_plan_id,
	'&{asof_date}'::date as asof_date
    from @&{stagename}/&{stagepath}/subscriptionoption.csv
);

alter table &{dbname}._STAGING.SUBSCRIPTIONOPTION swap with &{dbname}.AMS.SUBSCRIPTIONOPTION;
