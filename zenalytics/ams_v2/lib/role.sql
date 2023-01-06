---------------------------------------------------------------
------------------------- ROLE --------------------------------
---------------------------------------------------------------

use warehouse &{whname};

truncate table &{dbname}._STAGING.ROLE;

copy into  &{dbname}._STAGING.ROLE
from
(
	select 
	$1::integer as role_id,
	$2::timestamp as created,
	$3::timestamp as updated,
	$4 as title,
	'&{asof_date}'::date as asof_date
    from @&{stagename}/&{stagepath}/role.csv
);

alter table &{dbname}._STAGING.ROLE swap with &{dbname}.AMS.ROLE;
