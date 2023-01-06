-----------------------------------------------------------------------
--------------------- AMDASHBOARD_WALKTHROUGHS ------------------------
-----------------------------------------------------------------------

use database &{dbname};
use warehouse &{whname};
use role &{rolename};

truncate table &{dbname}._STAGING.AMDASHBOARD_WALKTHROUGHS;

copy into &{dbname}._STAGING.AMDASHBOARD_WALKTHROUGHS
from
(
	select
		$1 as business_id,
		$2::integer as walkthrough_past_thirty,
		$3::integer as walkthrough_total,
		'&{asof_date}'::date as asof_date
	FROM @&{stagename}/&{stagepath}/amdashboard_walkthroughs.csv
);

alter table &{dbname}._STAGING.AMDASHBOARD_WALKTHROUGHS swap with &{dbname}.AMS.AMDASHBOARD_WALKTHROUGHS;
