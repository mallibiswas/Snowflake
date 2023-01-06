-----------------------------------------------------------------------
---------------------- AMDASHBOARD_SMART_MESSAGE_USAGE -------------------------
-----------------------------------------------------------------------

use database &{dbname};
use warehouse &{whname};
use role &{rolename};

truncate table &{dbname}._STAGING.AMDASHBOARD_SMART_MESSAGE_USAGE;

copy into &{dbname}._STAGING.AMDASHBOARD_SMART_MESSAGE_USAGE
from
(
	select
		$1 as business_id,
		$2::integer as total_smart_messages_sent_past_thirty,
		$3::integer as total_blast_messages_sent_past_thirty,
		'&{asof_date}'::date as asof_date
	FROM @&{stagename}/&{stagepath}/amdashboard_smart_message_usage.csv
);

alter table &{dbname}._STAGING.AMDASHBOARD_SMART_MESSAGE_USAGE swap with &{dbname}.AMS.AMDASHBOARD_SMART_MESSAGE_USAGE;
