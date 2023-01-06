---------------------------------------------------------------
------------------------- PLANADDON ---------------------------
---------------------------------------------------------------

use warehouse &{whname};

truncate table &{dbname}._STAGING.PLANADDON;

copy into  &{dbname}._STAGING.PLANADDON
from
(
	select 
	$1::integer as planaddon_id,
	$2::timestamp as created,
	$3::timestamp as updated,
	$4 as recurly_code,
	$5::integer as plan_id,
	$6 as name,
	'&{asof_date}'::date as asof_date
    from @&{stagename}/&{stagepath}/planaddon.csv
);

alter table &{dbname}._STAGING.PLANADDON swap with &{dbname}.AMS.PLANADDON;
