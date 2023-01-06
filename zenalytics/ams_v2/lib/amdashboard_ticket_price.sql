-----------------------------------------------------------------------
--------------------- AMDASHBOARD_TICKET_PRICE ------------------------
-----------------------------------------------------------------------

use database &{dbname};
use warehouse &{whname};
use role &{rolename};

truncate table &{dbname}._STAGING.AMDASHBOARD_TICKET_PRICE;

copy into &{dbname}._STAGING.AMDASHBOARD_TICKET_PRICE
from
(
	select
		$1 as business_id,
		$2::integer as smart_messages_configured_count,
		'&{asof_date}'::date as asof_date
	FROM @&{stagename}/&{stagepath}/amdashboard_ticket_price.csv
);

alter table &{dbname}._STAGING.AMDASHBOARD_TICKET_PRICE swap with &{dbname}.AMS.AMDASHBOARD_TICKET_PRICE;
