-----------------------------------------------------------------------
--------------------- AMDASHBOARD_REP_MANAGEMENT ----------------------
-----------------------------------------------------------------------

use database &{dbname};
use warehouse &{whname};
use role &{rolename};


truncate table &{dbname}._STAGING.AMDASHBOARD_REP_MANAGEMENT;

copy into &{dbname}._STAGING.AMDASHBOARD_REP_MANAGEMENT
from
(
	select
		$1 as business_id,
		$2::integer as rep_mgmt_ratings_count,
		$3::integer as rep_mgmt_template_count,
		'&{asof_date}'::date as asof_date
	FROM @&{stagename}/&{stagepath}/amdashboard_rep_management.csv
);

alter table &{dbname}._STAGING.AMDASHBOARD_REP_MANAGEMENT swap with &{dbname}.AMS.AMDASHBOARD_REP_MANAGEMENT;
