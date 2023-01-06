-----------------------------------------------------------------------
----------------------- AMDASHBOARD_EVENT_LOGS ------------------------
-----------------------------------------------------------------------

use database &{dbname};
use warehouse &{whname};
use role &{rolename};

truncate table &{dbname}._STAGING.AMDASHBOARD_EVENT_LOGS;

copy into  &{dbname}._STAGING.AMDASHBOARD_EVENT_LOGS
from
(
	select 
		$1 as id,
		$2::timestamp as created,
		$3 as type,
		$4 as details,
		'&{asof_date}'::date as asof_date
	FROM @&{stagename}/&{stagepath}/amdashboard_event_logs.csv	
);

alter table &{dbname}._STAGING.AMDASHBOARD_EVENT_LOGS swap with &{dbname}.AMS.AMDASHBOARD_EVENT_LOGS;
