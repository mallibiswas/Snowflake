-----------------------------------------------------------------------
-------------------- AMDASHBOARD_EMAIL_IMPORTS ------------------------
-----------------------------------------------------------------------

use database &{dbname};
use warehouse &{whname};
use role &{rolename};

truncate table &{dbname}._STAGING.AMDASHBOARD_EMAIL_IMPORTS;

copy into  &{dbname}._STAGING.AMDASHBOARD_EMAIL_IMPORTS
from
(
	select 
		$1 as business_id,
		$2::integer as contact_list_import_failure_count,
		$3::integer as contact_list_import_success_count,
		'&{asof_date}'::date as asof_date
	FROM @&{stagename}/&{stagepath}/amdashboard_email_imports.csv	
);

alter table &{dbname}._STAGING.AMDASHBOARD_EMAIL_IMPORTS swap with &{dbname}.AMS.AMDASHBOARD_EMAIL_IMPORTS;
