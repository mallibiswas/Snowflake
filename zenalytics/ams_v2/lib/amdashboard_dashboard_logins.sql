-----------------------------------------------------------------------
-------------------- AMDASHBOARD_DASHBOARD_LOGINS -------------------------
-----------------------------------------------------------------------

use database &{dbname};
use warehouse &{whname};
use role &{rolename};


truncate table &{dbname}._STAGING.AMDASHBOARD_DASHBOARD_LOGINS;

copy into  &{dbname}._STAGING.AMDASHBOARD_DASHBOARD_LOGINS
from
(
	select 
		$1 as business_id,
		$2::integer as dashboard_days_accessed_count,
		$3::date as dashboard_first_access_date,
		$4::date as dashboard_last_access_date,
		$5 as parent_business_id,
		$6::integer as parent_days_accessed_count,
		$7::date as parent_first_access_date,
		$8::date as parent_last_access_date,
		'&{asof_date}'::date as asof_date
	FROM @&{stagename}/&{stagepath}/amdashboard_dashboard_logins.csv	
);

alter table &{dbname}._STAGING.AMDASHBOARD_DASHBOARD_LOGINS swap with &{dbname}.AMS.AMDASHBOARD_DASHBOARD_LOGINS;
