-----------------------------------------------------------------------
---------------------- AMDASHBOARD_SMART_MESSAGES_CONFIG -------------------------
-----------------------------------------------------------------------

use database &{dbname};
use warehouse &{whname};
use role &{rolename};

truncate table &{dbname}._STAGING.AMDASHBOARD_SMART_MESSAGES_CONFIG;

copy into &{dbname}._STAGING.AMDASHBOARD_SMART_MESSAGES_CONFIG
from
(
	select
		$1 as business_id,
		$2::integer as smart_messages_configured_count,
		'&{asof_date}'::date as asof_date
	FROM @&{stagename}/&{stagepath}/amdashboard_smart_messages_config.csv
);

alter table &{dbname}._STAGING.AMDASHBOARD_SMART_MESSAGES_CONFIG swap with &{dbname}.AMS.AMDASHBOARD_SMART_MESSAGES_CONFIG;
