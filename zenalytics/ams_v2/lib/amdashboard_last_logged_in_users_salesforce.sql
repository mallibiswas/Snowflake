-----------------------------------------------------------------------
------------ AMDASHBOARD_LAST_LOGGED_IN_USERS_SALESFORCE --------------
-----------------------------------------------------------------------

use database &{dbname};
use warehouse &{whname};
use role &{rolename};

truncate table &{dbname}._STAGING.AMDASHBOARD_LAST_LOGGED_IN_USERS_SALESFORCE;

copy into &{dbname}._STAGING.AMDASHBOARD_LAST_LOGGED_IN_USERS_SALESFORCE
from
(
	select 
		$1 as business_id,
		$2 as last_login_email_list,
		'&{asof_date}'::date as asof_date
	FROM @&{stagename}/&{stagepath}/amdashboard_last_logged_in_users_salesforce.csv	
);

alter table &{dbname}._STAGING.AMDASHBOARD_LAST_LOGGED_IN_USERS_SALESFORCE swap with &{dbname}.AMS.AMDASHBOARD_LAST_LOGGED_IN_USERS_SALESFORCE;
