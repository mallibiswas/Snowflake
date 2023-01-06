---------------------------------------------------------------
------------------------- STAFFUSERROLE -----------------------
---------------------------------------------------------------

use warehouse &{whname};

truncate table &{dbname}._STAGING.STAFFUSERROLE;

copy into  &{dbname}._STAGING.STAFFUSERROLE
from
(
	select 
	$1::integer as staffuserrole_id,
	$2::timestamp as created,
	$3::timestamp as updated,
	$4::integer as staff_user_id,
	$5::integer as role_id,
	$6::boolean as active,
	'&{asof_date}'::date as asof_date
    from @&{stagename}/&{stagepath}/staffuserrole.csv
);

alter table &{dbname}._STAGING.STAFFUSERROLE swap with &{dbname}.AMS.STAFFUSERROLE;
