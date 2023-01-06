-----------------------------------------------------------------------
---------------------- AMDASHBOARD_ROUTER_TYPES -----------------------
-----------------------------------------------------------------------

use database &{dbname};
use warehouse &{whname};
use role &{rolename};

truncate table &{dbname}._STAGING.AMDASHBOARD_ROUTER_TYPES;

copy into &{dbname}._STAGING.AMDASHBOARD_ROUTER_TYPES
from
(
	select
		$1 as business_id,
		$2 as router_types,
		'&{asof_date}'::date as asof_date
	FROM @&{stagename}/&{stagepath}/amdashboard_router_types.csv	
);

alter table &{dbname}._STAGING.AMDASHBOARD_ROUTER_TYPES swap with &{dbname}.AMS.AMDASHBOARD_ROUTER_TYPES;
