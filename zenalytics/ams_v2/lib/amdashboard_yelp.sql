-----------------------------------------------------------------------
--------------------- AMDASHBOARD_YELP ------------------------
-----------------------------------------------------------------------

use database &{dbname};
use warehouse &{whname};
use role &{rolename};

truncate table &{dbname}._STAGING.AMDASHBOARD_YELP;

copy into &{dbname}._STAGING.AMDASHBOARD_YELP
from
(
	select
		$1 as business_id,
		$2 as yelp_advertiser,
		'&{asof_date}'::date as asof_date
	FROM @&{stagename}/&{stagepath}/amdashboard_yelp.csv
);

alter table &{dbname}._STAGING.AMDASHBOARD_YELP swap with &{dbname}.AMS.AMDASHBOARD_YELP;
