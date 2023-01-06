---------------------------------------------------------------
------------------------- ROUTER ------------------------------
---------------------------------------------------------------

use database &{dbname};
use warehouse &{whname};
use role &{rolename};

truncate table &{dbname}._STAGING.ROUTER;

copy into  &{dbname}._STAGING.ROUTER
from
(
	select 
	$1::integer as router_surrogate_id,
	$2::timestamp as created,
	$3::timestamp as updated,
	$4::integer as account_id,
	$5::integer as location_id,
	$6 as businessprofile_id,
	$7 as mac,
	$8 as router_state,
	$9::date as installed,
	$10 as shipped,
	$11 as delivered,
	$12 as router_id,
	$13 as type,
	parse_json($14)::variant as vendor_data,	
	'&{asof_date}'::date as asof_date
    from @&{stagename}/&{stagepath}/router.csv
);

alter table &{dbname}._STAGING.ROUTER swap with &{dbname}.AMS.ROUTER;
