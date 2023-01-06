-----------------------------------------------------------------------
------------------ EVENT ----------------------
-----------------------------------------------------------------------

use database &{dbname};
use warehouse &{whname};
use role &{rolename};


truncate table &{dbname}._STAGING.EVENT;

copy into  &{dbname}._STAGING.EVENT
from
(
	select 
	$1::integer as event_id,
	$2::timestamp as created,
	$3::timestamp as updated,
	$4::integer as account_id,
	$5::integer as contract_id,
	$6::integer as location_id,
	$7 as event_name,
	$8::timestamp as event_time,
	$9 as user_id,
	$10 as entity_name,
	$11::integer as entity_id,
	parse_json($12)::variant as payload,
	$13::integer as staff_user_id,
	$14::integer as role_id,
	'&{asof_date}'::date as asof_date
    from @&{stagename}/&{stagepath}/event.csv
);

alter table &{dbname}._STAGING.EVENT swap with &{dbname}.AMS.EVENT;
