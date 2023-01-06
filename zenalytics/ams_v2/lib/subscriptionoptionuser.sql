-------------------------------------------------------
------------------ SUBSCRIPTIONOPTIONUSER -------------
-------------------------------------------------------

use database &{dbname};
use warehouse &{whname};
use role &{rolename};

truncate table &{dbname}._STAGING.SUBSCRIPTIONOPTIONUSER;

copy into  &{dbname}._STAGING.SUBSCRIPTIONOPTIONUSER
from
(
	select
		$1 as email,
		$2::timestamp as created,
		$3::timestamp as updated,
		$4 as group_code,
		'&{asof_date}'::date as asof_date
    from @&{stagename}/&{stagepath}/subscriptionoptionuser.csv
);

alter table &{dbname}._STAGING.SUBSCRIPTIONOPTIONUSER swap with &{dbname}.AMS.SUBSCRIPTIONOPTIONUSER;
