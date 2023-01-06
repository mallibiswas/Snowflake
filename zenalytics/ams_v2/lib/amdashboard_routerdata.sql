-----------------------------------------------------------------------
---------------------- AMDASHBOARD_ROUTERDATA -------------------------
-----------------------------------------------------------------------

use database &{dbname};
use warehouse &{whname};
use role &{rolename};

truncate table &{dbname}._STAGING.AMDASHBOARD_ROUTERDATA;

copy into &{dbname}._STAGING.AMDASHBOARD_ROUTERDATA
from
(
	select
		$1 as business_id,
		$2::date as router_last_probe_date,
		$3::date as router_install_date,
		$4::date as router_last_crm_request_date,
		'&{asof_date}'::date as asof_date
	FROM @&{stagename}/&{stagepath}/amdashboard_routerdata.csv
);

alter table &{dbname}._STAGING.AMDASHBOARD_ROUTERDATA swap with &{dbname}.AMS.AMDASHBOARD_ROUTERDATA;
