---------------------------------------------------------------
------------------------- STAFFUSER ---------------------------
---------------------------------------------------------------

use warehouse &{whname};

truncate table &{dbname}._STAGING.STAFFUSER;

copy into  &{dbname}._STAGING.STAFFUSER
from
(
	select 
	$1::integer as staff_user_id,
	$2::timestamp as created,
	$3::timestamp as updated,
	$4 as email,
	$5 as name,
	$6::date as start_date,
	$7::date as end_date,
	'&{asof_date}'::date as asof_date
    from @&{stagename}/&{stagepath}/staffuser.csv
);

alter table &{dbname}._STAGING.STAFFUSER swap with &{dbname}.AMS.STAFFUSER;
