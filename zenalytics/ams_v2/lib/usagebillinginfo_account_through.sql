-------------------------------------------------------
---------- USAGEBILLINGINFO_ACCOUNT_THROUGH -----------
-------------------------------------------------------

use warehouse &{whname};

truncate table &{dbname}._STAGING.USAGEBILLINGINFO_ACCOUNT_THROUGH;

copy into  &{dbname}._STAGING.USAGEBILLINGINFO_ACCOUNT_THROUGH
from
(
	select 
	$1::integer as usagebillinginfo_account_id,
	$2::integer as usagebillinginfo_id,
	$3::integer as account_id,
	'&{asof_date}'::date as asof_date
    from @&{stagename}/&{stagepath}/usagebillinginfo_account_through.csv
);

alter table &{dbname}._STAGING.USAGEBILLINGINFO_ACCOUNT_THROUGH swap with &{dbname}.AMS.USAGEBILLINGINFO_ACCOUNT_THROUGH;
