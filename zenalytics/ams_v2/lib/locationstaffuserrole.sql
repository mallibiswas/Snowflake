---------------------------------------------------------------
------------------ LOCATIONSTAFFUSERROLE ----------------------
---------------------------------------------------------------

use database &{dbname};
use warehouse &{whname};
use role &{rolename};

truncate table &{dbname}._STAGING.LOCATIONSTAFFUSERROLE;

copy into  &{dbname}._STAGING.LOCATIONSTAFFUSERROLE
from
(
	select 
	$1::integer as locationstaffuserrole_id,
	$2::timestamp as created,
	$3::timestamp as updated,
	$4::integer as location_id,
	$5::integer as staff_user_id,
	$6::integer as role_id,
	$7::boolean as active,
	'&{asof_date}'::date as asof_date
    from @&{stagename}/&{stagepath}/locationstaffuserrole.csv
);

alter table &{dbname}._STAGING.LOCATIONSTAFFUSERROLE swap with &{dbname}.AMS.LOCATIONSTAFFUSERROLE;
