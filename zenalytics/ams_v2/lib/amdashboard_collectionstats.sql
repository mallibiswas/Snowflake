-----------------------------------------------------------------------
-------------------- AMDASHBOARD_COLLECTION_STATS ---------------------
-----------------------------------------------------------------------

use database &{dbname};
use warehouse &{whname};
use role &{rolename};

truncate table &{dbname}._STAGING.AMDASHBOARD_COLLECTIONSTATS;

copy into  &{dbname}._STAGING.AMDASHBOARD_COLLECTIONSTATS
from
(
	select 
		$1 as business_id,
		$2::integer as total_emails_collected_all_time,
		$3::integer as average_emails_collected_all_time,
		$4::integer as total_emails_collected_past_thirty,
		$5::integer as average_emails_collected_past_thirty,
		'&{asof_date}'::date as asof_date
	FROM @&{stagename}/&{stagepath}/amdashboard_collection_stats.csv
);

alter table &{dbname}._STAGING.AMDASHBOARD_COLLECTIONSTATS swap with &{dbname}.AMS.AMDASHBOARD_COLLECTIONSTATS;
