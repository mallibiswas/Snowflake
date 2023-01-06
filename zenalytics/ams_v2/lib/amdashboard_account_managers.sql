-----------------------------------------------------------------------
------------------ AMDASHBOARD_ACCOUNT_MANAGERS -----------------------
-----------------------------------------------------------------------

use database &{dbname};
use warehouse &{whname};
use role &{rolename};

truncate table &{dbname}._STAGING.AMDASHBOARD_ACCOUNT_MANAGERS;

copy into  &{dbname}._STAGING.AMDASHBOARD_ACCOUNT_MANAGERS
from
(
	select
		$1 as business_id,
		$2 as salesforce_id,
		$3 as account_manager_email,
		$4 as account_manager,
		$5 as account_manager_manager,
		$6::date as last_activity_date,
		'&{asof_date}'::date as asof_date
    from @&{stagename}/&{stagepath}/amdashboard_account_managers.csv
);

alter table &{dbname}._STAGING.AMDASHBOARD_ACCOUNT_MANAGERS swap with &{dbname}.AMS.AMDASHBOARD_ACCOUNT_MANAGERS;
