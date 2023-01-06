----------------------------------------------------------
------------------------- KILLSWITCH ------------------------
----------------------------------------------------------

use database &{dbname};
use warehouse &{whname};
use role &{rolename};

truncate table &{dbname}._STAGING.KILLSWITCH;

copy into  &{dbname}._STAGING.KILLSWITCH
from
(
	select
		$1 as id,
		$2::timestamp as created,
		$3::timestamp as updated,
		$4 as name,
		$5 as killed,
		'&{asof_date}'::date as asof_date
    from @&{stagename}/&{stagepath}/killswitch.csv
);

alter table &{dbname}._STAGING.KILLSWITCH swap with &{dbname}.AMS.KILLSWITCH;
